package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "embedded":
		err = runEmbedded(os.Args[2:])
	case "durable":
		err = runDurable(os.Args[2:])
	case "distributed":
		err = runDistributed(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		err = fmt.Errorf("unknown example %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "complete example failed:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: go run ./complete <embedded|durable|distributed> [flags]")
}
