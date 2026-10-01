"""Valida que todos los DAGs del repo carguen sin errores."""
import glob
import importlib.util
import os
import pytest
import sys

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")


def get_dag_files():
    return glob.glob(os.path.join(DAGS_DIR, "*.py"))


@pytest.mark.parametrize("dag_file", get_dag_files())
def test_dag_imports_without_errors(dag_file):
    """Cada archivo DAG debe poder importarse sin excepciones."""
    module_name = os.path.splitext(os.path.basename(dag_file))[0]
    spec = importlib.util.spec_from_file_location(module_name, dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "dag") or any(
        hasattr(module, attr) for attr in dir(module) if "dag" in attr.lower()
    )
