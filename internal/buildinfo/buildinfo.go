// Package buildinfo holds version metadata injected at build time via
// -ldflags (see the Makefile's `build` target). Defaults are used for local
// `go run`/`go build` invocations that don't pass ldflags.
package buildinfo

var (
	// Version is the git tag/describe output for this build.
	Version = "dev"
	// Commit is the short git commit SHA for this build.
	Commit = "none"
)
