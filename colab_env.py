"""
Colab/local bootstrap helper for the retail DWH project.

Usage (Colab):
    python colab_env.py --install-postgres --copy-from-drive

Usage (local Linux):
    python colab_env.py --install-postgres
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path


DATA_DIR = Path("/content/data")
DRIVE_DIR = Path("/content/drive/MyDrive/retail_dw_data")


def run(cmd: str) -> None:
    subprocess.run(cmd, shell=True, check=True)


def cleanup_processes() -> None:
    run("pkill -f psql || true")
    run("pkill -f postgres || true")


def install_postgres() -> None:
    run("apt-get update -y")
    run("apt-get install -y postgresql postgresql-contrib")
    run("pg_ctlcluster 14 main start || true")
    run("sudo -u postgres psql -c \"select version();\"")
    run("sudo -u postgres psql -c \"show data_directory;\"")
    run("sudo -u postgres psql -c \"select now();\"")


def mount_drive_if_available() -> None:
    try:
        from google.colab import drive  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("google.colab is not available in this environment.") from exc

    drive.mount("/content/drive")


def copy_datasets_from_drive() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    files = [
        "01_empty_95_off.csv",
        "02_empty_95_on.csv",
        "03_empty_5_off.csv",
        "04_empty_5_on.csv",
    ]
    for file_name in files:
        src = DRIVE_DIR / file_name
        dst = DATA_DIR / file_name
        if not src.exists():
            raise FileNotFoundError(f"Dataset not found on Drive: {src}")
        shutil.copy(src, dst)

    run("sudo chmod o+rx /content || true")
    run(f"sudo chmod o+rx {DATA_DIR} || true")
    for file_name in files:
        run(f"sudo chmod o+r {DATA_DIR / file_name} || true")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap retail DWH environment.")
    parser.add_argument("--cleanup", action="store_true", help="Stop existing psql/postgres processes.")
    parser.add_argument("--install-postgres", action="store_true", help="Install and initialize PostgreSQL.")
    parser.add_argument("--copy-from-drive", action="store_true", help="Mount Google Drive and copy CSV datasets.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.cleanup:
        cleanup_processes()

    if args.install_postgres:
        install_postgres()

    if args.copy_from_drive:
        mount_drive_if_available()
        copy_datasets_from_drive()

    if not any([args.cleanup, args.install_postgres, args.copy_from_drive]):
        print("No action requested. Use --help for available options.")


if __name__ == "__main__":
    main()
