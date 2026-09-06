// Module: the comparison benchmarks, kept out of the root module so that
// sanecache itself keeps its promise of no dependencies.
module github.com/andared/sanecache/benchmarks

go 1.24.0

replace github.com/andared/sanecache => ../

require (
	github.com/andared/sanecache v0.1.0
	github.com/dgraph-io/ristretto/v2 v2.4.2
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/jellydator/ttlcache/v3 v3.4.1
	github.com/maypok86/otter/v2 v2.3.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
