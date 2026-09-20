import numpy as np
import xarray as xr

from eo_ml_pipeline.utils.io import save_xarray


def test_save_xarray(tmp_path):
    """Save an xarray dataset as Zarr."""

    ds = xr.Dataset({"var": (("x",), [1, 2, 3])})

    path = save_xarray(ds, tmp_path, "test")

    assert path == f"{tmp_path}/test.zarr"

def test_save_xarray_tif(tmp_path):
    """Save a georeferenced xarray DataArray as GeoTIFF."""

    ds = xr.DataArray(
        np.ones((2, 2)),
        dims=("y", "x"),
        coords={
            "x": [0, 1],
            "y": [0, 1],
        },
        name="var",
    )

    ds = ds.rio.write_crs("EPSG:4326")

    path = save_xarray(
        ds,
        tmp_path,
        "test",
        save_format="tif",
    )

    assert path == f"{tmp_path}/test.tif"