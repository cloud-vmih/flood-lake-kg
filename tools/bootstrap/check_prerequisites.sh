#!/bin/sh
set -eu

PYTHON_BIN=${PYTHON:-python3.11}

require_command() {
    command_name=$1
    install_hint=$2
    if command -v "$command_name" >/dev/null 2>&1; then
        return
    fi
    printf '%s\n' "Missing required command: $command_name. $install_hint" >&2
    exit 1
}

require_command git "Install Git before running make bootstrap."
require_command make "Install GNU Make before running make bootstrap."
require_command openssl "Install OpenSSL before running make bootstrap."
require_command docker "Install Docker Engine or enable Docker Desktop WSL integration."
require_command "$PYTHON_BIN" "Install Python 3.11 or run make bootstrap PYTHON=/path/to/python3.11."

"$PYTHON_BIN" -c '
import sys
if sys.version_info[:2] != (3, 11):
    raise SystemExit(f"Python 3.11 required, got {sys.version.split()[0]}")
'

if ! docker compose version >/dev/null 2>&1; then
    printf '%s\n' 'Docker Compose v2 is required.' >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    cat >&2 <<'MESSAGE'
Docker CLI is installed but the daemon is unavailable.
On Windows, start Docker Desktop and enable WSL integration for this distribution.
On Linux, start Docker Engine and grant the current user access to the Docker socket.
MESSAGE
    exit 1
fi

printf '%s\n' 'Host prerequisites are ready.'
