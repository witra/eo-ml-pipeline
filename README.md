# EO ML Pipeline
Modular Earth Observation data pipeline for generating ML-ready dataset with Airflow and AI-assisted orchestration

## Installation

### Requirements

* Python 3.12 or newer
* [uv](https://docs.astral.sh/uv/) — recommended for dependency management

The package is currently available directly from GitHub and has not yet been published to PyPI.

### Install with uv

You can install the package directly from the GitHub repository:

```bash
uv pip install "git+https://github.com/witra/eo-ml-pipeline"
```

To install a specific branch:

```bash
uv pip install "git+https://github.com/witra/eo-ml-pipeline@main"
```

Or install a specific Git tag or commit:

```bash
uv pip install "git+https://github.com/witra/eo-ml-pipeline@v0.1.0"
```

### Using it in an existing uv project

If you are already using `uv` to manage your own project, you can add `eo-ml-pipeline` as a Git dependency:

```bash
uv add git+https://github.com/witra/eo-ml-pipeline
```

This will add the Git repository as a dependency in your project's `pyproject.toml`.

You can then import the package normally:

```python
from eo_ml_pipeline.discovery.stac import search_items
from eo_ml_pipeline.acquisition.stac import acquire_items
from eo_ml_pipeline.processing.preprocessing import apply_preprocessing
from eo_ml_pipeline.dataset.base import construct_xy
```

### Install with pip

If you are not using `uv`, the package can also be installed directly from GitHub with pip:

```bash
pip install "git+https://github.com/witra/eo-ml-pipeline"
```

### Verify the installation

After installation, verify that the package can be imported:

```bash
python -c "import eo_ml_pipeline; print(eo_ml_pipeline.__file__)"
```

You should see the location of the installed `eo_ml_pipeline` package.

## Development Installation

To work on the repository itself, clone the project and use `uv`:

```bash
git clone https://github.com/witra/eo-ml-pipeline.git
cd eo-ml-pipeline
uv sync --dev
```

The development environment is created automatically in `.venv` by `uv`. You can run commands inside the environment with `uv run`.

For example:

```bash
uv run pytest
```

Run tests with coverage:

```bash
uv run pytest \
    --cov=eo_ml_pipeline \
    --cov-report=term-missing \
    --cov-report=xml
```

## Reference Pipelines

The repository also contains reference implementations showing how the package components can be combined into end-to-end EO processing workflows.

For example:

```bash
uv run python reference_pipelines/sensor_label_pair.py
```

See [`reference_pipelines/`](./reference_pipelines/) for available pipelines and their configuration.

## Airflow

The project also provides Airflow-based workflow orchestration. Airflow is installed as part of the project's dependencies.

After setting up the development environment:

```bash
uv run airflow standalone
```

This starts a local Airflow instance for development and experimentation.

See the `dags/` directory for example DAGs.
