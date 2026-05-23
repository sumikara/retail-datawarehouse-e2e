from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


CORE_SQL_FOLDERS = [
    "sql/00_setup",
    "sql/01_landing",
    "sql/02_mapping",
    "sql/03_normalized",
    "sql/04_dimensions",
    "sql/05_orchestrastion",
    "sql/07_data_quality",
]

CRITICAL_SETUP_FILES = [
    "sql/00_setup/01_extensions_schemas.sql",
    "sql/00_setup/02_orchestration_metadata.sql",
    "sql/00_setup/03_etl_log.sql",
]

DQ_TEST_SUITE_FILES = [
    "sql/07_data_quality/test_cases/01_layer_test_cases.sql",
    "sql/07_data_quality/test_cases/02_entity_test_cases.sql",
    "sql/07_data_quality/test_cases/03_system_test_suite.sql",
    "sql/07_data_quality/test_cases/04_dq_test_levels_master_suite.sql",
]


def test_repo_structure_exists():
    assert (REPO_ROOT / "sql").exists()
    assert (REPO_ROOT / "docs").exists()
    assert (REPO_ROOT / "README.md").exists()


def test_core_sql_layer_folders_exist():
    for folder in CORE_SQL_FOLDERS:
        assert (REPO_ROOT / folder).exists()


def test_critical_setup_sql_files_exist():
    for filepath in CRITICAL_SETUP_FILES:
        assert (REPO_ROOT / filepath).exists()


def test_dq_sql_suites_exist():
    for filepath in DQ_TEST_SUITE_FILES:
        assert (REPO_ROOT / filepath).exists()


def test_readme_mentions_hybrid_architecture():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8").lower()
    assert "hybrid" in readme
    assert "inmon-kimball" in readme


def test_readme_mentions_project_identity_terms():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "PostgreSQL" in readme
    assert "PL/pgSQL" in readme
    assert "Hybrid Inmon-Kimball" in readme
    assert "data quality" in readme.lower()
