package modules

import (
	"bufio"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"os/exec"
	"syscall"
	"time"
)

var _ Module = (*Binary)(nil)

type Binary struct {
	Base
	binPath                  string
	args                     []string
	gracefulShutdownInterval time.Duration
	cmd                      *exec.Cmd
	exitCh                   chan struct{}
}

func NewBinary(binPath string, args []string, gracefulShutdownInterval ...time.Duration) *Binary {
	gracefulTime := 15 * time.Second
	if len(gracefulShutdownInterval) > 0 {
		gracefulTime = gracefulShutdownInterval[0]
	}
	return &Binary{
		binPath:                  binPath,
		args:                     args,
		gracefulShutdownInterval: gracefulTime,
		exitCh:                   make(chan struct{}),
	}
}

func (b *Binary) GetExitChannel() chan struct{} {
	return b.exitCh
}
func (b *Binary) Init() error {
	b.cmd = exec.Command(b.binPath, b.args...)
	stdout, _ := b.cmd.StdoutPipe()
	stdOutScanner := bufio.NewScanner(stdout)
	stderr, _ := b.cmd.StderrPipe()
	stdErrScanner := bufio.NewScanner(stderr)
	go func() {
		for stdOutScanner.Scan() {
			logger.Log.Info(stdOutScanner.Text())
		}
	}()
	go func() {
		for stdErrScanner.Scan() {
			logger.Log.Error(stdErrScanner.Text())
		}
	}()
	err := b.cmd.Start()
	go func() {
		b.cmd.Wait()
		close(b.exitCh)
	}()
	return err
}
func (b *Binary) Shutdown() error {
	err := b.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		return err
	}
	timeout := time.After(b.gracefulShutdownInterval)
	select {
	case <-b.exitCh:
		return nil
	case <-timeout:
		b.cmd.Process.Kill()
		return ErrTimeoutTerminatingBinaryModule
	}
}
