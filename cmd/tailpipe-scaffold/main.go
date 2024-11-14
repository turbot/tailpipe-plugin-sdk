package main

import (
	"log"
)

func main() {
	if err := Execute(); err != nil {
		log.Fatalf("could not execute command: %v", err)
	}
}
