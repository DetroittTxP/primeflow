//go:build windows

package worker

import (
	"os"
	"os/exec"
)

// isolate is a no-op: there is no POSIX process group to leave.
func isolate(*exec.Cmd) {}

// terminate kills outright. Windows has no signal a child could settle its run
// on, so a cancelled run is left to the janitor's crash path instead.
func terminate(p *os.Process) error { return p.Kill() }
