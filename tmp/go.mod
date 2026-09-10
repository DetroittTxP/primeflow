// This directory is air's rebuild scratch — binaries, build logs, and whatever
// a container run leaves behind under a bind mount. None of it is Go source
// this module wants compiled, and a Go module cache dumped in here once was
// enough to break `go test ./...` for everyone:
//
//	pattern ./...: directory tmp/gomodcache/github.com/… outside main module
//
// A go.mod makes the tree a nested module, which the `./...` pattern skips
// whole. Nothing imports it and nothing builds it; it exists so that what lands
// here stays out of the main module's way. .gitignore keeps this one file and
// ignores the rest of the directory.
module primeflow.local/scratch

go 1.25
