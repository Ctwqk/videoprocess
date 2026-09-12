package channelops

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigOwnedHistorySecretFile(t *testing.T) {
	const secret = "redis://owned-reader:fixture-password@127.0.0.1:6380/0"
	for _, newline := range []string{"", "\n"} {
		t.Run(map[bool]string{false: "exact", true: "trailing_newline"}[newline != ""], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "owned-history-secret")
			if err := os.WriteFile(path, []byte(secret+newline), 0o400); err != nil {
				t.Fatal(err)
			}
			t.Setenv("OWNED_HISTORY_REDIS_URL_FILE", " \t"+path+" \t")
			t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
			t.Setenv("CHANNELOPS_RUNNER_ID", "owned-secret-test")
			t.Setenv("CHANNELOPS_LIVE_MODE", "false")
			cfg := LoadConfig()
			if cfg.OwnedHistoryRedisURL != secret {
				t.Fatal("mounted secret did not take precedence over REDIS_URL")
			}
			if err := cfg.Validate(); err != nil {
				t.Fatal("valid mounted secret failed configuration validation")
			}
		})
	}
}

func TestLoadConfigOwnedHistorySecretFileEmptyCompatibility(t *testing.T) {
	for _, kind := range []string{"absent", "empty", "whitespace"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("OWNED_HISTORY_REDIS_URL_FILE", "")
			if kind == "absent" {
				if err := os.Unsetenv("OWNED_HISTORY_REDIS_URL_FILE"); err != nil {
					t.Fatal(err)
				}
			} else if kind == "whitespace" {
				t.Setenv("OWNED_HISTORY_REDIS_URL_FILE", " \t\n")
			}
			t.Setenv("CHANNELOPS_RUNNER_ID", "owned-secret-test")
			t.Setenv("CHANNELOPS_LIVE_MODE", "false")
			for _, legacy := range []string{"redis://127.0.0.1:6379/0", ""} {
				t.Setenv("REDIS_URL", " \t"+legacy+" \t")
				cfg := LoadConfig()
				if cfg.OwnedHistoryRedisURL != legacy || cfg.Validate() != nil {
					t.Fatal("empty file setting changed legacy REDIS_URL behavior")
				}
			}
		})
	}
}

func TestLoadConfigOwnedHistorySecretFileRejectsBadFile(t *testing.T) {
	const secret = "redis://owned-reader:fixture-password@127.0.0.1:6380/0"
	for _, kind := range []string{"missing", "directory", "symlink", "mode_0600", "empty", "newline_only", "nul", "invalid_utf8", "oversize", "malformed_url", "anonymous", "default_user", "missing_password", "query", "fragment", "invalid_db"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sensitive-path-fixture-password")
			content, mode := []byte(secret), os.FileMode(0o400)
			switch kind {
			case "mode_0600":
				mode = 0o600
			case "empty":
				content = nil
			case "newline_only":
				content = []byte("\n")
			case "nul":
				content = append(content, 0)
			case "invalid_utf8":
				content = append(content, 0xff)
			case "oversize":
				content = []byte(strings.Repeat("x", 4097))
			case "malformed_url":
				content = []byte("redis://owned-reader:fixture-password@%")
			case "anonymous":
				content = []byte("redis://127.0.0.1:6380/0")
			case "default_user":
				content = []byte("redis://default:fixture-password@127.0.0.1:6380/0")
			case "missing_password":
				content = []byte("redis://owned-reader@127.0.0.1:6380/0")
			case "query":
				content = []byte(secret + "?read_timeout=0")
			case "fragment":
				content = []byte(secret + "#fixture-password")
			case "invalid_db":
				content = []byte("redis://owned-reader:fixture-password@127.0.0.1:6380/16")
			}
			if kind == "directory" {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			} else if kind != "missing" {
				if err := os.WriteFile(path, content, mode); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					link := path + "-link"
					if err := os.Symlink(path, link); err != nil {
						t.Fatal(err)
					}
					path = link
				}
			}
			t.Setenv("OWNED_HISTORY_REDIS_URL_FILE", path)
			t.Setenv("REDIS_URL", secret)
			t.Setenv("CHANNELOPS_RUNNER_ID", "owned-secret-test")
			t.Setenv("CHANNELOPS_LIVE_MODE", "false")
			cfg := LoadConfig()
			err := cfg.Validate()
			if err == nil || err.Error() != "OWNED_HISTORY_REDIS_URL_FILE is invalid" {
				t.Fatal("bad explicit file did not fail with the static configuration error")
			}
			if cfg.OwnedHistoryRedisURL != "" {
				t.Fatal("bad explicit file retained content or fell back to REDIS_URL")
			}
			if _, err := NewRunner(context.Background(), cfg); err == nil || err.Error() != "OWNED_HISTORY_REDIS_URL_FILE is invalid" {
				t.Fatal("NewRunner did not reject bad file before database access")
			}
		})
	}
}
