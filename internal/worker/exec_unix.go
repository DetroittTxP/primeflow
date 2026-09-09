//go:build !windows

package worker

import (
	"os"
	"os/exec"
	"syscall"
)

// isolate puts the child in its own process group, so a Ctrl-C in a terminal
// stops the worker without cancelling the runs it is draining. Under an
// orchestrator only PID 1 is signalled, which is the same shape.
func isolate(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// terminate asks a child to stop — the same request an inline worker makes by
// cancelling a run's context, which is why the outcomes are the same.
func terminate(p *os.Process) error { return p.Signal(syscall.SIGTERM) }
