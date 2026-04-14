from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_repo_structure_exists():
    assert (REPO_ROOT / "sql").exists()
    assert (REPO_ROOT / "docs").exists()
    assert (REPO_ROOT / "README.md").exists()


def test_dq_sql_suites_exist():
    assert (REPO_ROOT / "sql/07_data_quality/test_cases/01_layer_test_cases.sql").exists()
    assert (REPO_ROOT / "sql/07_data_quality/test_cases/02_entity_test_cases.sql").exists()
    assert (REPO_ROOT / "sql/07_data_quality/test_cases/03_system_test_suite.sql").exists()


def test_readme_mentions_hybrid_architecture():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "hybrid warehouse architecture" in readme.lower()
