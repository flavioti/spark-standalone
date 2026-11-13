#!/usr/bin/env bash
# Prepare everything needed to run the Spark notebooks from this machine:
#   1. Java 17 (required by Spark), installed with apt if missing
#   2. Project virtualenv at <project>/.venv with PySpark 4.0.0 and ipykernel
#   3. Jupyter kernel "Python (spark-learning)" pointing to that virtualenv
#
# Usage:
#   scripts/setup_env.sh
#
# Environment variables you can override:
#   PYTHON          Python interpreter for the venv (default: a 3.13 if found, else python3)
#   JAVA_PACKAGE    apt package to install if Java is missing (default: openjdk-17-jdk-headless)

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
PYSPARK_VERSION="4.0.0"
KERNEL_NAME="spark-learning"
JAVA_PACKAGE="${JAVA_PACKAGE:-openjdk-17-jdk-headless}"

die() {
    echo "error: $*" >&2
    exit 1
}

step() {
    echo
    echo "==> $*"
}

# 1. Java
step "Checking Java"
if command -v java >/dev/null 2>&1; then
    java -version 2>&1 | head -1
else
    echo "Java not found, installing $JAVA_PACKAGE (sudo password may be requested)"
    sudo apt-get update
    sudo apt-get install -y "$JAVA_PACKAGE"
    command -v java >/dev/null 2>&1 || die "Java installation failed"
fi

# 2. Python interpreter for the venv
step "Checking Python"
if [ -z "${PYTHON:-}" ]; then
    PYTHON="python3"
    # Prefer a 3.13 install: PySpark 4.0.0 does not support 3.14 yet
    for candidate in "$HOME"/.pyenv/versions/3.13*/bin/python3.13 python3.13; do
        if [ -x "$candidate" ] && "$candidate" --version >/dev/null 2>&1; then
            PYTHON="$candidate"
            break
        fi
    done
fi
command -v "$PYTHON" >/dev/null 2>&1 || die "Python not found: $PYTHON"

PY_VERSION="$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
case "$PY_VERSION" in
    3.9|3.10|3.11|3.12|3.13) ;;
    *) die "Python $PY_VERSION is not supported by PySpark $PYSPARK_VERSION. Set PYTHON to a 3.9-3.13 interpreter." ;;
esac
echo "Using $PYTHON (Python $PY_VERSION)"

# 3. Virtualenv and packages
step "Setting up virtualenv at $VENV_DIR"
if [ ! -x "$VENV_DIR/bin/python" ]; then
    "$PYTHON" -m venv "$VENV_DIR"
else
    echo "Virtualenv already exists, updating packages"
fi
"$VENV_DIR/bin/python" -m pip install --quiet --upgrade pip
"$VENV_DIR/bin/python" -m pip install --quiet "pyspark==$PYSPARK_VERSION" ipykernel

# 4. Jupyter kernel for VS Code
step "Registering Jupyter kernel"
"$VENV_DIR/bin/python" -m ipykernel install --user \
    --name "$KERNEL_NAME" --display-name "Python (spark-learning)" >/dev/null

step "Done"
echo "Java:        $(command -v java)"
echo "Interpreter: $VENV_DIR/bin/python"
echo "PySpark:     $("$VENV_DIR/bin/python" -c 'import pyspark; print(pyspark.__version__)')"
echo "Kernel:      Python (spark-learning)"
echo
echo "In VS Code, select the interpreter $VENV_DIR/bin/python or the kernel 'Python (spark-learning)'."
