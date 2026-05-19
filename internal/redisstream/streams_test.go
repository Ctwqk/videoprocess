package redisstream

import "testing"

func TestTaskStream(t *testing.T) {
	if got := TaskStream("ffmpeg_go"); got != "vp:tasks:ffmpeg_go" {
		t.Fatalf("TaskStream = %q", got)
	}
}
