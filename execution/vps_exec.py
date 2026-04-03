#!/usr/bin/env python3
"""Run shell commands on a VPS via SSH using environment variables."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv

    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    FALLBACK_ENV_PATH = None
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

import paramiko


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _connect() -> paramiko.SSHClient:
    host = _required("VPS_HOST")
    user = _required("VPS_USER")
    password = _required("VPS_PASSWORD")
    port = int(os.getenv("VPS_PORT", "22"))

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        host,
        port=port,
        username=user,
        password=password,
        look_for_keys=False,
        allow_agent=False,
        timeout=30,
        auth_timeout=30,
        banner_timeout=30,
    )
    return client


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Execute a remote shell command on VPS over SSH."
    )
    parser.add_argument(
        "command",
        help="Remote shell command to run. Example: 'systemctl status mybot'",
    )
    args = parser.parse_args()

    client = _connect()
    try:
        _stdin, stdout, stderr = client.exec_command(args.command, timeout=300)
        out = stdout.read()
        err = stderr.read()
        if out:
            sys.stdout.buffer.write(out)
        if err:
            sys.stderr.buffer.write(err)
        return stdout.channel.recv_exit_status()
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
