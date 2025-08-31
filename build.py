#!/usr/bin/env python3
import os, sys, subprocess, shutil
from pathlib import Path
import importlib.util

APP_SCRIPT = "app.py"
APP_NAME = "ExcelConsolidator"
ICON = "icon.ico"
README = "README.txt"

def safe_rmtree(p):
    if os.path.exists(p):
        try:
            shutil.rmtree(p)
            print(f"Cleaned {p}")
        except Exception as e:
            print(f"Warn: could not remove {p}: {e}")

def create_readme():
    content = """Excel Consolidator
==================

This application consolidates multiple Excel files into Cumulative and Centralizator sheets.

Features:
- Select Excel files (.xlsx, .xlsm)
- Flexible header detection
- Copies original sheets, preserves basic formatting
- Aggregation with stock integration

Usage:
1) Launch the app
2) Add files with "Add Files"
3) Click "Process Files" and choose output
4) Find the result in the selected location

System Requirements:
- Windows 10 or later
"""
    with open(README, "w", encoding="utf-8") as f:
        f.write(content)
    print("Created README.txt")

def build_executable():
    # Clean previous builds
    for d in ["build", "dist", "__pycache__"]:
        safe_rmtree(d)

    # PyInstaller base command
    py = sys.executable
    addsep = os.pathsep  # ';' on Windows, ':' elsewhere

    cmd = [
        py, "-m", "PyInstaller",
        "--onefile",
        "--windowed",
        f"--name={APP_NAME}",
        f"--distpath=dist",
        f"--workpath=build",
        "--clean",
        # collect openpyxl and its xml dependency robustly
        "--collect-all", "openpyxl",
        "--collect-all", "et_xmlfile",
        # include README alongside the exe unpack dir
        f"--add-data={README}{addsep}.",
        APP_SCRIPT,
    ]

    if os.path.exists(ICON):
        cmd.insert(-1, f"--icon={ICON}")

    # Optional: only add tkinter hidden import if module is present (usually not needed)
    if importlib.util.find_spec("tkinter") is not None:
        pass  # PyInstaller detects tkinter automatically

    print("Building executable with PyInstaller...")
    print("Command:", " ".join(cmd))

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Build successful.")
        dist_path = Path("dist")
        if dist_path.exists():
            print("\nFiles in dist:")
            for f in dist_path.iterdir():
                try:
                    sz = f.stat().st_size / (1024 * 1024)
                    print(f"  - {f.name} ({sz:.1f} MB)")
                except Exception:
                    print(f"  - {f.name}")
        return True
    except subprocess.CalledProcessError as e:
        print("Build failed.")
        if e.stdout: print("STDOUT:\n", e.stdout)
        if e.stderr: print("STDERR:\n", e.stderr)
        return False

def main():
    print("Excel Consolidator Build Script")
    print("=" * 40)

    # venv check
    if hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix:
        print("✓ Virtual environment detected")
    else:
        print("⚠ Not in a virtual environment")

    if not os.path.exists(APP_SCRIPT):
        print(f"❌ {APP_SCRIPT} not found.")
        return

    create_readme()

    # Ensure PyInstaller
    try:
        subprocess.run([sys.executable, "-m", "PyInstaller", "--version"], check=True,
                       capture_output=True, text=True)
        print("✓ PyInstaller available")
    except Exception:
        print("Installing PyInstaller...")
        subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller"], check=True)

    ok = build_executable()
    print("\n✓ Build completed." if ok else "\n❌ Build failed.")

if __name__ == "__main__":
    main()
