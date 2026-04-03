#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import posixpath
import sys
from pathlib import Path

import paramiko
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REMOTE_ROOT = "/root/projects/tryon-bot"
DEFAULT_SERVICE = "tryon-bot"


def _load_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def _required(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _connect() -> paramiko.SSHClient:
    host = _required("VPS_HOST")
    user = _required("VPS_USER")
    password = _required("VPS_PASSWORD")
    port = int((os.getenv("VPS_PORT") or "22").strip())

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


def _run(client: paramiko.SSHClient, command: str, timeout: int = 240) -> tuple[int, str, str]:
    _stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    code = stdout.channel.recv_exit_status()
    return code, out, err


def _emit(text: str) -> None:
    if not text:
        return
    encoding = sys.stdout.encoding or "utf-8"
    data = text.encode(encoding, errors="replace")
    sys.stdout.buffer.write(data)
    if not text.endswith("\n"):
        sys.stdout.buffer.write(b"\n")


def _ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    parts: list[str] = []
    for part in remote_dir.strip("/").split("/"):
        if not part:
            continue
        parts.append(part)
        current = "/" + "/".join(parts)
        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def _upload_file(sftp: paramiko.SFTPClient, local_path: Path, remote_path: str) -> None:
    _ensure_remote_dir(sftp, posixpath.dirname(remote_path))
    sftp.put(str(local_path), remote_path)
    print(f"uploaded file: {local_path} -> {remote_path}")


def _upload_tree(sftp: paramiko.SFTPClient, local_dir: Path, remote_dir: str) -> None:
    for path in local_dir.rglob("*"):
        if path.is_dir():
            continue
        if "__pycache__" in path.parts:
            continue
        if path.suffix == ".pyc":
            continue
        if path.suffix == ".tmp":
            continue
        rel = path.relative_to(local_dir).as_posix()
        remote_path = posixpath.join(remote_dir, rel)
        _upload_file(sftp, path, remote_path)


def _write_remote_file(client: paramiko.SSHClient, remote_path: str, content: str) -> None:
    sftp = client.open_sftp()
    try:
        _ensure_remote_dir(sftp, posixpath.dirname(remote_path))
        with sftp.file(remote_path, "w") as f:
            f.write(content)
    finally:
        sftp.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy curtain bot to VPS and restart systemd service.")
    parser.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    parser.add_argument("--service", default=DEFAULT_SERVICE)
    parser.add_argument("--logs", type=int, default=120)
    args = parser.parse_args()

    _load_env()

    local_src = PROJECT_ROOT / "src"
    local_refs = PROJECT_ROOT / "\u0420\u0435\u0444\u0435\u0440\u0435\u043d\u0441\u044b \u0448\u0442\u043e\u0440"
    local_preview = PROJECT_ROOT / "\u041f\u0440\u0435\u0432\u044c\u044e"
    local_directives = PROJECT_ROOT / "directives"
    local_execution = PROJECT_ROOT / "execution"
    local_env = PROJECT_ROOT / ".env"
    local_req = PROJECT_ROOT / "requirements.txt"
    local_readme = PROJECT_ROOT / "README.md"
    local_example_room = PROJECT_ROOT / "\u041f\u0440\u0438\u043c\u0435\u0440 \u043a\u0430\u043a\u043e\u0435 \u0444\u043e\u0442\u043e \u043d\u0443\u0436\u043d\u043e \u0441\u043a\u0438\u043d\u0443\u0442\u044c.png"
    local_example_mount = PROJECT_ROOT / "\u041f\u0440\u0438\u043c\u0435\u0440 \u043f\u0440\u043e\u0435\u043c- \u0441\u0442\u0432\u043e\u0440\u043a\u0438.png"

    missing = [p for p in (local_src, local_refs, local_preview, local_directives, local_execution, local_env, local_req) if not p.exists()]
    if missing:
        raise SystemExit("Missing local paths: " + ", ".join(str(p) for p in missing))

    client = _connect()
    try:
        # Ensure root directory
        code, out, err = _run(client, f"mkdir -p '{args.remote_root}'")
        if code != 0:
            raise RuntimeError(f"Cannot create remote root: {err or out}")

        # Remove stale reference catalog on remote to avoid old/deleted folders lingering.
        code, out, err = _run(client, f"rm -rf '{args.remote_root}/ReferenceStore'")
        if code != 0:
            raise RuntimeError(f"Cannot reset remote ReferenceStore: {err or out}")
        code, out, err = _run(client, f"rm -rf '{args.remote_root}/PreviewStore'")
        if code != 0:
            raise RuntimeError(f"Cannot reset remote PreviewStore: {err or out}")

        sftp = client.open_sftp()
        try:
            _upload_tree(sftp, local_src, f"{args.remote_root}/src")
            _upload_tree(sftp, local_refs, f"{args.remote_root}/ReferenceStore")
            _upload_tree(sftp, local_preview, f"{args.remote_root}/PreviewStore")
            _upload_tree(sftp, local_directives, f"{args.remote_root}/directives")
            _upload_tree(sftp, local_execution, f"{args.remote_root}/execution")
            _upload_file(sftp, local_env, f"{args.remote_root}/.env")
            _upload_file(sftp, local_req, f"{args.remote_root}/requirements.txt")
            if local_readme.exists():
                _upload_file(sftp, local_readme, f"{args.remote_root}/README.md")
            if local_example_room.exists():
                _upload_file(sftp, local_example_room, f"{args.remote_root}/РџСЂРёРјРµСЂ РєР°РєРѕРµ С„РѕС‚Рѕ РЅСѓР¶РЅРѕ СЃРєРёРЅСѓС‚СЊ.png")
            if local_example_mount.exists():
                _upload_file(sftp, local_example_mount, f"{args.remote_root}/РџСЂРёРјРµСЂ РїСЂРѕРµРј- СЃС‚РІРѕСЂРєРё.png")
        finally:
            sftp.close()

        setup_cmd = (
            f"cd '{args.remote_root}' && "
            "python3 -m venv .venv && "
            ". .venv/bin/activate && "
            "pip install -U pip && "
            "pip install -r requirements.txt && "
            "python -m py_compile src/simple_curtain_bot.py src/reference_store_catalog.py"
        )
        code, out, err = _run(client, setup_cmd, timeout=1200)
        if code != 0:
            raise RuntimeError(f"Remote setup failed:\n{out}\n{err}")
        print("remote_setup: ok")

        service_content = f"""[Unit]
Description=Curtain Telegram Bot (NanoBanana)
After=network.target

[Service]
Type=simple
WorkingDirectory={args.remote_root}
EnvironmentFile={args.remote_root}/.env
ExecStart={args.remote_root}/.venv/bin/python {args.remote_root}/src/simple_curtain_bot.py
Restart=always
RestartSec=3
User=root

[Install]
WantedBy=multi-user.target
"""
        service_path = f"/etc/systemd/system/{args.service}.service"
        _write_remote_file(client, service_path, service_content)

        code, out, err = _run(
            client,
            f"systemctl daemon-reload && systemctl enable {args.service} && systemctl restart {args.service} && systemctl is-active {args.service}",
            timeout=240,
        )
        if code != 0:
            raise RuntimeError(f"Service restart failed:\n{out}\n{err}")
        print(f"service_active: {out.strip()}")

        code, out, err = _run(client, f"systemctl status {args.service} --no-pager | sed -n '1,30p'")
        if code == 0:
            print("service_status:")
            _emit(out.rstrip())
        else:
            print("service_status_error:")
            _emit((err or out).rstrip())

        code, out, err = _run(client, f"journalctl -u {args.service} -n {max(1, args.logs)} --no-pager")
        if code == 0:
            print("service_logs:")
            _emit(out.rstrip())
        else:
            print("service_logs_error:")
            _emit((err or out).rstrip())
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


