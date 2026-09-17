from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "tools/bootstrap/init_lakehouse_env.sh"
REQUIRED = {
    "LAKEHOUSE_POSTGRES_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "POLARIS_DB_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "POLARIS_CLIENT_ID",
    "POLARIS_CLIENT_SECRET",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_API_SECRET_KEY",
    "AIRFLOW_ADMIN_USERNAME",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_UID",
}


def parse_env(path: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def test_example_declares_blank_lakehouse_secrets() -> None:
    values = parse_env(ROOT / ".env.example")

    assert REQUIRED <= values.keys()
    assert all(values[key] == "" for key in REQUIRED)


def test_initializer_preserves_values_and_is_idempotent(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("FLASHFLOOD_CDSE_USERNAME=kept\nMINIO_ROOT_USER=existing\n")
    data_root = tmp_path / "lakehouse"
    env = os.environ | {"ENV_FILE": str(env_file), "LAKEHOUSE_DATA_ROOT": str(data_root)}

    first = subprocess.run([SCRIPT], env=env, text=True, capture_output=True, check=True)
    before = env_file.read_bytes()
    second = subprocess.run([SCRIPT], env=env, text=True, capture_output=True, check=True)

    values = parse_env(env_file)
    assert values["FLASHFLOOD_CDSE_USERNAME"] == "kept"
    assert values["MINIO_ROOT_USER"] == "existing"
    assert REQUIRED <= values.keys()
    assert all(values[key] for key in REQUIRED)
    assert before == env_file.read_bytes()
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert first.stdout == second.stdout == "Lakehouse local environment is ready.\n"
    assert all(value not in first.stdout + first.stderr for value in values.values())
    assert {p.relative_to(data_root).as_posix() for p in data_root.rglob("*") if p.is_dir()} >= {
        "airflow",
        "airflow/logs",
        "staging",
        "minio",
        "postgres",
    }
