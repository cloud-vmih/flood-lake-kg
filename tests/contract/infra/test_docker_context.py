from pathlib import Path

ROOT = Path(__file__).parents[3]


def dockerignore_lines() -> set[str]:
    path = ROOT / ".dockerignore"
    assert path.is_file()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def test_docker_context_excludes_local_state_and_large_documents() -> None:
    assert {
        "dataset/",
        ".venv/",
        ".git/",
        ".worktrees/",
        "**/__pycache__/",
        ".pytest_cache/",
        "docs/*.pdf",
        "docs/*.docx",
        "docs/*.pptx",
        "docs/*.xlsx",
    } <= dockerignore_lines()


def test_docker_context_does_not_exclude_build_inputs() -> None:
    lines = dockerignore_lines()
    assert "requirements/" not in lines
    assert "infra/" not in lines
    assert "spark/jobs/" not in lines
