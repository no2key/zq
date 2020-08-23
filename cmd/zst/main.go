package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "github.com/brimsec/zq/cmd/zst/create"
	_ "github.com/brimsec/zq/cmd/zst/inspect"
	"github.com/brimsec/zq/cmd/zst/root"
)

func main() {
	//XXX Seed
	rand.Seed(time.Now().UTC().UnixNano())
	_, err := root.Zst.ExecRoot(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
