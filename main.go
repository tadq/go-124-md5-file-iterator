package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

var path string

func init() {
	flag.StringVar(&path, "path", "", "path to MD5")
}

// TASK: Iterate over root path using recursion and feed results into iterator.
// Make sure context cancellation and iteration cancellation is respected.
//
// Big part of the code taken from golang.org/x/sync/errgroup and adopted for iteration.
func main() {
	flag.Parse()

	if path == "" {
		slog.Info("missing -path in input")
		return
	}

	for p, md5 := range MD5All(context.Background(), path) {
		fmt.Println(p, md5)
	}
}

type result struct {
	path string
	sum  [md5.Size]byte
}

// MD5All reads all the files in the file tree rooted at root and returns a map
// from file path to the MD5 sum of the file's contents. If the directory walk
// fails or any read operation fails, MD5All returns an error.
func MD5All(ctx context.Context, root string) iter.Seq2[string, [md5.Size]byte] {
	return func(yield func(string, [md5.Size]byte) bool) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// ctx is canceled when g.Wait() returns. When this version of MD5All returns
		// - even in case of error! - we know that all of the goroutines have finished
		// and the memory they were using can be garbage-collected.
		g, ctx := errgroup.WithContext(ctx)
		paths := make(chan string)

		g.Go(func() error {
			defer close(paths)
			return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() {
					return nil
				}
				select {
				case paths <- path:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			})
		})

		// Start a fixed number of goroutines to read and digest files.
		c := make(chan result)
		const numDigesters = 20
		for i := 0; i < numDigesters; i++ {
			g.Go(func() error {
				for path := range paths {
					data, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					select {
					case c <- result{path, md5.Sum(data)}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
		}
		go func() {
			g.Wait()
			close(c)
		}()

		for r := range c {
			if !yield(r.path, r.sum) {
				cancel()
				return
			}
		}
		// Check whether any of the goroutines failed. Since g is accumulating the
		// errors, we don't need to send them (or check for them) in the individual
		// results sent on the channel.
		if err := g.Wait(); err != nil {
			return
		}
		return
	}
}
