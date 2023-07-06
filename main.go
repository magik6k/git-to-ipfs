package main

import (
	"bytes"
	"context"
	"fmt"
	rpc "github.com/filecoin-project/kubo-api-client"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"io"
	"os"
	"sync"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

const concurrencyLimit = 1

var cidRawPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   multihash.SHA2_256,
	MhLength: -1, // default length
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <path>\n", os.Args[0])
		os.Exit(1)
	}

	repo, err := git.PlainOpen(os.Args[1])
	if err != nil {
		panic(err)
	}

	localApi, err := rpc.NewLocalApi()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	ibs, err := blockstore.NewLocalIPFSBlockstore(ctx, false)
	if err != nil {
		panic(err)
	}

	type mapent struct {
		cid  cid.Cid
		size uint64
	}

	var entLk sync.Mutex
	ents := map[plumbing.Hash]mapent{}

	//ignore := map[plumbing.Hash]struct{}{}

	// Create a channel to handle concurrent operations
	operationChannel := make(chan plumbing.Hash, concurrencyLimit)
	var wg sync.WaitGroup

	var gitLk sync.Mutex

	throttleAdd := make(chan struct{}, 128)

	// Use go routines to handle the work
	for i := 0; i < concurrencyLimit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for hash := range operationChannel {
				gitLk.Lock()
				blob, err := repo.BlobObject(hash)
				gitLk.Unlock()
				if err != nil {
					panic(err)
				}
				reader, err := blob.Reader()
				if err != nil {
					panic(err)
				}
				d, err := io.ReadAll(reader)
				if err != nil {
					panic(err)
				}

				if len(d) < 1<<20 {
					var raw blocks.Block
					rawCid, err := cidRawPrefix.Sum(d)
					if err != nil {
						panic(err)
					}

					raw, err = blocks.NewBlockWithCid(d, rawCid)
					if err != nil {
						panic(err)
					}

					throttleAdd <- struct{}{}
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() { <-throttleAdd }()

						err = ibs.Put(ctx, raw)
						if err != nil {
							panic(err)
						}
					}()

					//fmt.Printf("Writing %s %s\n", hash.String(), rawCid.String())

					entLk.Lock()
					ents[hash] = mapent{
						cid:  rawCid,
						size: uint64(len(d)),
					}
					entLk.Unlock()
				} else {
					ar, err := localApi.Unixfs().Add(ctx, files.NewReaderFile(bytes.NewReader(d)),
						options.Unixfs.Chunker("size-1048576"),
						options.Unixfs.Pin(false),
						options.Unixfs.RawLeaves(true),
						options.Unixfs.CidVersion(1),
						options.Unixfs.Hash(multihash.SHA2_256),
						options.Unixfs.Progress(false),
					)
					if err != nil {
						panic(err)
					}

					//fmt.Printf("Writing %s %s\n", hash.String(), ar.Cid().String())

					entLk.Lock()
					ents[hash] = mapent{
						cid:  ar.Cid(),
						size: uint64(len(d)),
					}
					entLk.Unlock()
				}

				/*
					err = os.WriteFile(hash.String(), content, 0644)
					if err != nil {
						panic(err)
					}*/
			}
		}()
	}

	bi, err := repo.BlobObjects()
	if err != nil {
		panic(err)
	}

	gitLk.Lock()
	err = bi.ForEach(func(b *object.Blob) error {
		gitLk.Unlock()
		operationChannel <- b.Hash
		gitLk.Lock()
		return nil
	})
	gitLk.Unlock()
	if err != nil {
		panic(err)
	}

	close(operationChannel)
	wg.Wait()

	////////////////////////
	// TREES

	ti, err := repo.TreeObjects()
	if err != nil {
		panic(err)
	}

	var treeList []plumbing.Hash
	solved := map[plumbing.Hash]struct{}{}

	type need struct {
		need map[plumbing.Hash]*int64
	}

	// found -> need
	deps := map[plumbing.Hash]need{}

	err = ti.ForEach(func(t *object.Tree) error {
		if len(t.Entries) > 10_000 {
			return xerrors.Errorf("tree %s has too many entries", t.Hash.String())
		}

		dep := map[plumbing.Hash]struct{}{}
		for _, e := range t.Entries {
			if e.Mode == filemode.Submodule {
				continue
			}

			_, ok := ents[e.Hash]
			if ok {
				continue
			}
			_, ok = solved[e.Hash]
			if ok {
				continue
			}

			dep[e.Hash] = struct{}{}
		}

		if len(dep) == 0 { // have everything

			var solveWant func(h plumbing.Hash) error

			solveWant = func(h plumbing.Hash) error {

				// mark as solved
				treeList = append(treeList, h)
				solved[h] = struct{}{}

				// solve deps
				if solved, ok := deps[h]; ok {

					for k, v := range solved.need {
						*v--
						if *v == 0 {
							err := solveWant(k)
							if err != nil {
								return err
							}
						}
					}

					delete(deps, h)
				}

				return nil
			}

			err := solveWant(t.Hash)
			if err != nil {
				return err
			}
		} else {
			needRefs := int64(len(dep))

			for k := range dep {
				_, ok := deps[k]
				if !ok {
					deps[k] = need{
						need: map[plumbing.Hash]*int64{t.Hash: &needRefs},
					}
				} else {
					deps[k].need[t.Hash] = &needRefs
				}
			}
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	if len(deps) > 0 {
		//panic(fmt.Sprintf("unresolved deps: %d", len(deps)))
	}

	err = func() error {
		for _, hash := range treeList {
			pnd := unixfs.EmptyDirNode()

			var size uint64

			t, err := repo.TreeObject(hash)
			if err != nil {
				return xerrors.Errorf("getting tree %s: %w", hash.String(), err)
			}

			for _, e := range t.Entries {
				if e.Mode == filemode.Submodule {
					continue
				}

				en, ok := ents[e.Hash]
				if !ok {
					return xerrors.Errorf("missing hash ent %s, hash %s", e.Name, e.Hash.String())
				}

				size += en.size

				lnk := format.Link{
					Size: en.size,
					Cid:  en.cid,
				}

				err := pnd.AddRawLink(e.Name, &lnk)
				if err != nil {
					return xerrors.Errorf("adding link: %w", err)
				}
			}

			entLk.Lock()
			ents[t.Hash] = mapent{
				cid:  pnd.Cid(),
				size: size,
			}
			entLk.Unlock()

			throttleAdd <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-throttleAdd }()

				err = ibs.Put(ctx, pnd)
				if err != nil {
					panic(err)
				}
			}()

			//fmt.Printf("Writing TREE %s %s\n", hash.String(), pnd.Cid().String())
		}

		return nil
	}()
	if err != nil {
		panic(err)
	}

	////////////////////////
	// COMMITS

	ci, err := repo.CommitObjects()
	if err != nil {
		panic(err)
	}

	commShards := map[string]*merkledag.ProtoNode{}
	commShardSz := map[string]uint64{}
	totalSize := uint64(0)

	commShard := func(h plumbing.Hash) string {
		return h.String()[:3]
	}

	commRem := func(h plumbing.Hash) string {
		return h.String()[3:]
	}

	err = ci.ForEach(func(c *object.Commit) error {
		if _, ok := commShards[commShard(c.Hash)]; !ok {
			commShards[commShard(c.Hash)] = unixfs.EmptyDirNode()
		}

		tree, ok := ents[c.TreeHash]
		if !ok {
			//return xerrors.Errorf("missing tree %s", c.TreeHash.String())
			fmt.Fprintf(os.Stderr, "missing tree %s\n", c.TreeHash.String())
			return nil
		}

		totalSize += tree.size
		commShardSz[commShard(c.Hash)] += tree.size

		return commShards[commShard(c.Hash)].AddRawLink(commRem(c.Hash), &format.Link{
			Size: tree.size,
			Cid:  tree.cid,
		})
	})
	if err != nil {
		panic(err)
	}

	root := unixfs.EmptyDirNode()

	for n, v := range commShards {
		if err := ibs.Put(ctx, v); err != nil {
			panic(err)
		}

		err := root.AddNodeLink(n, v)
		if err != nil {
			panic(err)
		}
	}

	if err := ibs.Put(ctx, root); err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", root.Cid().String())
}
