"""Build script for creating the ETL_Load executable."""
import subprocess
import sys


def build():
    cmd = [sys.executable, "-m", "PyInstaller", "etl_load.spec", "--clean"]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print("Build complete! Output in dist/ETL_Load/")


if __name__ == "__main__":
    build()
