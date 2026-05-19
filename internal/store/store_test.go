package store

import "testing"

func TestMimeForExtension(t *testing.T) {
	cases := map[string]string{
		".mp4": "video/mp4",
		".mkv": "video/x-matroska",
		".wav": "audio/wav",
		".srt": "application/x-subrip",
		".bin": "video/mp4",
	}
	for ext, want := range cases {
		if got := GuessMime(ext); got != want {
			t.Fatalf("GuessMime(%q) = %q; want %q", ext, got, want)
		}
	}
}
