import shutil
from pathlib import Path


def delete_path(path: str | Path):
    path = Path(path)
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.is_file():
        path.unlink()