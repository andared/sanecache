package main

import (
	"fmt"
	"time"

	"github.com/andared/sanecache"
)

func main() {
	c := sanecache.New(sanecache.Options[string, string]{
		TTL:        time.Minute,
		MaxEntries: 100,
	})
	defer c.Close()

	if err := c.Set("greeting", "hello"); err != nil {
		panic(err)
	}

	value, ok := c.Get("greeting")
	fmt.Println(value, ok)
}
