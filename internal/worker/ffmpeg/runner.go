package ffmpeg

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

type Runner struct {
	Binary string
}

func NewRunner() Runner {
	return Runner{Binary: "ffmpeg"}
}

func (r Runner) Run(ctx context.Context, args []string) (string, error) {
	binary := r.Binary
	if binary == "" {
		binary = "ffmpeg"
	}
	fullArgs := append([]string{"-y", "-hide_banner"}, args...)
	cmd := exec.CommandContext(ctx, binary, fullArgs...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stderr.String(), fmt.Errorf("ffmpeg failed: %w: %s", err, tail(stderr.String(), 2000))
	}
	return stderr.String(), nil
}

func tail(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[len(value)-max:]
}
