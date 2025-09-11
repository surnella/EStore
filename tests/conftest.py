# tests/conftest.py
import importlib.util
import pathlib
import sys

SRC_PATH = pathlib.Path(__file__).parent.parent / "src"

for py_file in SRC_PATH.rglob("*.py"):
    if py_file.name == "__init__.py":
        continue
    rel_path = py_file.relative_to(SRC_PATH).with_suffix("")
    module_name = ".".join(rel_path.parts)
    if module_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(module_name, py_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)


# import pytest
# import builtins
# from unittest.mock import patch
# import db.dbsql

# def pytest_generate_tests(metafunc):
#     if "db_mode" in metafunc.fixturenames:
#         metafunc.parametrize("db_mode", ["real", "fake"], scope="function")

# @pytest.fixture(autouse=True)
# def patch_db(request, db_mode):
#     """Automatically patch DB depending on db_mode"""
#     builtins.db_mode = db_mode  # 👈 global, visible in all tests

#     if db_mode == "fake":
#         def fake_session():
#             class FakeSession:
#                 def execute(self, stmt):
#                     raise Exception("DB boom (simulated)")
#                 def commit(self): pass
#                 def rollback(self): pass
#                 def close(self): pass
#             return FakeSession()

#         with patch("db.dbsql.SessionLocal", side_effect=fake_session):
#             yield
#     else:
#         # Use real DB
#         yield

