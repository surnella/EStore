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
