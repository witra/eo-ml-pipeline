import xarray as xr
import numpy as np

from eo_ml_pipeline.dataset.base import construct_xy

def test_construct_xy(tmp_path, monkeypatch):
    """Construct and save an aligned X/Y dataset."""
    x = xr.Dataset(
        {"B04": (("y", "x"), np.arange(36).reshape(6, 6))},
        coords={"x": np.arange(6), "y": np.arange(6)},
    ).rio.write_crs("EPSG:4326")

    y = xr.DataArray(
        np.ones((6, 6)),
        dims=("y", "x"),
        coords={"x": np.arange(6), "y": np.arange(6)},
        name="label",
    ).rio.write_crs("EPSG:4326")

    monkeypatch.setattr(
        "eo_ml_pipeline.utils.io.save_xarray",
        lambda ds, save_dir, basename, save_format, **kwargs:
            str(tmp_path / f"{basename}.{save_format}"),
    )

    result, path = construct_xy(
        x=x,
        y=y,
        pad=1,
        save_dir=str(tmp_path),
        basename="test",
        nodata=0,
    )

    assert result["B04"].shape == (4, 4)
    assert result["label"].shape == (4, 4)
    assert "B04" in result
    assert "label" in result
    assert path == str(tmp_path / "test.zarr")