#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT_DIR/docker-compose.yml"
NGINX_CONF="$ROOT_DIR/frontend/nginx.conf"

bash -n "$ROOT_DIR/deploy/macos/common.sh"

assert_contains() {
  local needle="$1"
  if ! grep -Fq "$needle" "$COMPOSE"; then
    printf 'FAIL: expected docker-compose.yml to contain %s\n' "$needle" >&2
    exit 1
  fi
}

assert_not_contains() {
  local needle="$1"
  if grep -Fq "$needle" "$COMPOSE"; then
    printf 'FAIL: expected docker-compose.yml not to contain %s\n' "$needle" >&2
    exit 1
  fi
}

assert_contains '${PLATFORM_UPLOAD_ROOT:-../constructure-platform-upload}/YouTubeManager'
assert_contains '${PLATFORM_UPLOAD_ROOT:-../constructure-platform-upload}/PlatformBrowserManager'
assert_contains '${PLATFORM_UPLOAD_RUNTIME_ROOT:-../constructure-platform-upload}/YouTubeManager/credentials'
assert_contains '${PLATFORM_UPLOAD_RUNTIME_ROOT:-../constructure-platform-upload}/PlatformBrowserManager/browser-profiles'
assert_contains '${YOUTUBE_MANAGER_PORT:-18999}:8899'
assert_not_contains '../../platform-upload'

if ! grep -Fq 'host.docker.internal:18999' "$NGINX_CONF"; then
  printf 'FAIL: expected frontend nginx to proxy YouTubeManager through host.docker.internal:18999\n' >&2
  exit 1
fi
