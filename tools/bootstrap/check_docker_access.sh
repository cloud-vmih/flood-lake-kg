#!/bin/sh
set -eu

if ! command -v docker >/dev/null 2>&1; then
    printf '%s\n' 'Không tìm thấy Docker CLI. Hãy cài Docker Engine trước.' >&2
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    printf '%s\n' 'Không tìm thấy Docker Compose v2.' >&2
    exit 1
fi

if docker info >/dev/null 2>&1; then
    exit 0
fi

cat >&2 <<'MESSAGE'
Không thể kết nối Docker daemon. Nếu tài khoản chưa thuộc nhóm docker, hãy tự chạy:
  sudo usermod -aG docker "$USER"
Sau đó đăng xuất và đăng nhập lại (hoặc khởi động lại máy), rồi chạy lại lệnh make.
Script này không tự chạy sudo.
MESSAGE
exit 1
