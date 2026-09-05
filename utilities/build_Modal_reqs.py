#!/usr/bin/env python3
"""
build_modal_reqs.py

Scans a directory of .py files for top-level imports and writes a
minimal requirements file (only third-party packages actually used).
Safe to run repeatedly — always overwrites the output file fresh.

Usage:
    python build_modal_reqs.py                     # scans ./pipeline, writes modal_reqs.txt
    python build_modal_reqs.py pipeline modal_reqs.txt
    python build_modal_reqs.py some_dir some_output.txt --pin
"""

import ast
import sys
import argparse
import pathlib
import importlib.metadata as ilm

STDLIB = sys.stdlib_module_names if hasattr(sys, "stdlib_module_names") else set()

IMPORT_TO_PYPI = {
    "cv2": "opencv-python",
    "yaml": "PyYAML",
    "PIL": "Pillow",
    "sklearn": "scikit-learn",
    "dotenv": "python-dotenv",
    "psycopg2": "psycopg2-binary",
    "google": "google-api-python-client",
}

def find_py_files(root: pathlib.Path):
    return list(root.rglob("*.py"))

def top_level_imports(py_file: pathlib.Path):
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    except SyntaxError as e:
        print(f"  ⚠ skipping {py_file} (syntax error: {e})")
        return set()

    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
    return names

def is_local_module(name: str, root: pathlib.Path):
    return (root / f"{name}.py").exists() or (root / name / "__init__.py").exists()

def resolve_pypi_name(import_name: str):
    return IMPORT_TO_PYPI.get(import_name, import_name)

def installed_version(pypi_name: str):
    try:
        return ilm.version(pypi_name)
    except ilm.PackageNotFoundError:
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scan_dir", nargs="?", default="pipeline")
    parser.add_argument("output", nargs="?", default="modal_reqs.txt")
    parser.add_argument("--pin", action="store_true")
    args = parser.parse_args()

    root = pathlib.Path(args.scan_dir).resolve()
    if not root.exists():
        print(f"❌ {root} does not exist")
        sys.exit(1)

    py_files = find_py_files(root)
    print(f"Scanning {len(py_files)} .py files under {root}")

    all_imports = set()
    for f in py_files:
        all_imports |= top_level_imports(f)

    third_party = set()
    skipped_local = set()
    skipped_stdlib = set()

    for name in sorted(all_imports):
        if name in STDLIB or name in {"__future__"}:
            skipped_stdlib.add(name)
            continue
        if is_local_module(name, root) or is_local_module(name, root.parent):
            skipped_local.add(name)
            continue
        third_party.add(name)

    lines = []
    unresolved = []
    for name in sorted(third_party):
        pypi_name = resolve_pypi_name(name)
        version = installed_version(pypi_name)
        if version is None:
            unresolved.append((name, pypi_name))
            continue
        lines.append(f"{pypi_name}=={version}" if args.pin else pypi_name)

    out_path = pathlib.Path(args.output)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"\n✅ Wrote {len(lines)} packages to {out_path}")
    print(f"   (skipped {len(skipped_stdlib)} stdlib, {len(skipped_local)} local modules)")

    if unresolved:
        print(f"\n⚠ Could not resolve installed version for {len(unresolved)} import(s) —")
        print("  either not installed in this venv, or the import→PyPI name mapping is wrong.")
        for imp, pypi in unresolved:
            print(f"    import {imp}  (tried package name: {pypi})")

if __name__ == "__main__":
    main()
