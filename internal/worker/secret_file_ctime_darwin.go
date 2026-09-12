//go:build darwin

package worker

import (
	"os"
	"syscall"
)

func secretFileChangeTime(
	info os.FileInfo,
) (secretChangeTime, bool) {
	metadata, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return secretChangeTime{}, false
	}
	return secretChangeTime{
		seconds:     metadata.Ctimespec.Sec,
		nanoseconds: metadata.Ctimespec.Nsec,
	}, true
}
