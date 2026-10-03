#!/usr/bin/env bash

set -euo pipefail

TEST_ENV="/tmp/eo-ml-pipeline-install-test"

echo "==> Building package"
uv build

echo "==> Creating clean virtual environment"
rm -rf "$TEST_ENV"
uv venv "$TEST_ENV"

PYTHON="$TEST_ENV/bin/python"

echo "==> Installing wheel"
uv pip install --python "$PYTHON" dist/*.whl

echo "==> Testing public API"
"$PYTHON" -c "
from eo_ml_pipeline.discovery import search_items
from eo_ml_pipeline.acquisition import acquire_items
from eo_ml_pipeline.processing import apply_preprocessing
from eo_ml_pipeline.dataset import construct_xy

print('All public imports OK')
"

echo "==> Checking installed version"
"$PYTHON" -c "
import importlib.metadata

version = importlib.metadata.version('eo-ml-pipeline')
print(f'Installed version: {version}')
"

# echo "==> Checking Airflow is not installed"
# if "$PYTHON" -c "import airflow" 2>/dev/null; then
#     echo "ERROR: Airflow should not be installed in the core package environment"
#     exit 1
# fi

echo "==> Installation test passed"
