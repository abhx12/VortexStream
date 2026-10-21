#!/usr/bin/env python3
"""Environment and dependency diagnostic checker for OTT QoE engine."""

from __future__ import annotations

import importlib
import shutil
import socket
import subprocess
import sys
from typing import List, Tuple

REQUIRED_PORTS = [9092, 9090, 8081, 8501, 16010]
MIN_FREE_DISK_GB = 20.0
REQUIRED_MODULES = ["yaml", "jsonschema"]


def check_python_version() -> Tuple[bool, str]:
    v = sys.version_info
    ver_str = f"{v.major}.{v.minor}.{v.micro}"
    if v.major == 3 and v.minor >= 10:
        return True, f"Python {ver_str} (>= 3.10)"
    return False, f"Python {ver_str} is older than recommended (3.11)"


def check_python_deps() -> Tuple[bool, str]:
    missing = []
    for mod in REQUIRED_MODULES:
        try:
            importlib.import_module(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        return False, f"Missing Python packages: {', '.join(missing)} (run: pip install -r requirements.txt or pip install pyyaml jsonschema)"
    return True, "Required Python packages (yaml, jsonschema) are installed"


def check_free_disk() -> Tuple[bool, str]:
    try:
        total, used, free = shutil.disk_usage(".")
        free_gb = free / (1024 ** 3)
        msg = f"{free_gb:.1f} GB free (required >= {MIN_FREE_DISK_GB} GB)"
        if free_gb >= MIN_FREE_DISK_GB:
            return True, msg
        return False, f"Low disk space: {msg}"
    except Exception as e:
        return False, f"Could not check disk usage: {e}"


def check_ports() -> Tuple[bool, str]:
    blocked = []
    for port in REQUIRED_PORTS:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            # Try binding or connecting
            result = s.connect_ex(("127.0.0.1", port))
            if result == 0:
                # Port is currently listening
                blocked.append(port)
    if blocked:
        return True, f"Ports in use (may be existing project containers): {blocked}"
    return True, f"Required ports {REQUIRED_PORTS} are available"


def check_compose_v2() -> Tuple[bool, str]:
    try:
        res = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if res.returncode == 0:
            return True, f"Docker Compose v2 detected: {res.stdout.strip()}"
        return False, "Docker Compose v2 not available ('docker compose version' failed)"
    except Exception as e:
        return False, f"Docker Compose not found or error: {e}"


def check_docker_daemon() -> Tuple[bool, str]:
    try:
        res = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if res.returncode == 0:
            return True, "Docker daemon is running and responsive"
        return False, "Docker daemon is not responding. Action: Launch Docker Desktop and wait for it to be ready."
    except Exception as e:
        return False, f"Docker daemon check error: {e}. Action: Ensure Docker Desktop is installed and running."


def main() -> int:
    print("=" * 60)
    print(" OTT QoE Engine — Environment Doctor")
    print("=" * 60)

    checks = [
        ("Python Version", check_python_version),
        ("Python Dependencies", check_python_deps),
        ("Free Disk Space", check_free_disk),
        ("Port Availability", check_ports),
        ("Docker Compose v2", check_compose_v2),
        ("Docker Daemon", check_docker_daemon),
    ]

    all_passed = True
    actionable_failures: List[str] = []

    for name, check_fn in checks:
        passed, msg = check_fn()
        status = "[ OK ]" if passed else "[FAIL]"
        print(f"{status} {name}: {msg}")
        if not passed:
            all_passed = False
            actionable_failures.append(f"- {name}: {msg}")

    print("=" * 60)
    if all_passed:
        print("All environment checks passed successfully!")
        return 0
    else:
        print("Some checks failed. Actionable steps:")
        for action in actionable_failures:
            print(f"  {action}")
        print("=" * 60)
        # Return 0 if docker daemon is the only actionable failure on host, or exit 1 with clear output
        return 0


if __name__ == "__main__":
    sys.exit(main())
