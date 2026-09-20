import xarray as xr
from datetime import datetime
import pystac
import numpy as np

from eo_ml_pipeline.acquisition.stac import acquire_s2_pc, acquire_items

def make_item(dt):
    """Create a minimal STAC item for testing."""
    return pystac.Item(
        id=dt.strftime("%Y%m%d"),
        geometry=None,
        bbox=None,
        datetime=dt,
        properties={},
    )

def test_acquire_s2_pc_empty_items(tmp_path, caplog):
    """Return 0 when no Sentinel-2 items are available."""
    result = acquire_s2_pc(
        bbox=(13, 52, 14, 53),
        items=[],
        save_dir=str(tmp_path),
        basename="test",
    )

    assert result == 0
    assert "No S2 scenes found" in caplog.text

def test_acquire_s2_pc(monkeypatch, tmp_path):
    """Load Sentinel-2 items and save the dataset."""
    item = make_item(datetime(2026, 1, 2))
    ds = xr.Dataset(
        {"B04": (("y", "x"), np.ones((2, 2)))},
        coords={"x": [0, 1], "y": [0, 1]},
    )

    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.pc.sign",
        lambda item: item,
    )
    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.stac_load",
        lambda items, **kwargs: ds,
    )
    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.bbox_to_epsg",
        lambda *bbox: 32633,
    )
    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.save_xarray",
        lambda ds, save_dir, basename, save_format, **kwargs:
            str(tmp_path / f"{basename}.{save_format}"),
    )

    result, path = acquire_s2_pc(
        bbox=(13, 52, 14, 53),
        items=[item],
        save_dir=str(tmp_path),
        basename="test",
        chunks={"x": 2, "y": 2},
    )

    assert isinstance(result, xr.Dataset)
    assert "B04" in result
    assert path == str(tmp_path / "test.zarr")
    assert result.rio.crs.to_epsg() == 32633

def test_acquire_s2_pc_chunks_to_encoding(monkeypatch, tmp_path):
    """Create Zarr encoding from chunk sizes."""
    item = make_item(datetime(2026, 1, 2))

    ds = xr.Dataset(
        {
            "B04": (("y", "x"), np.ones((2, 2))),
        },
        coords={"x": [0, 1], "y": [0, 1], "time": [0]},
    )

    load_kwargs = {}

    def mock_stac_load(items, **kwargs):
        load_kwargs.update(kwargs)
        return ds
    def mock_save_xarray(*args, **kwargs):
        load_kwargs.update(kwargs)
        return str(tmp_path/"test.zarr")

    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.pc.sign",
        lambda item: item,
    )
    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.stac_load",
        mock_stac_load,
    )
    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.bbox_to_epsg",
        lambda *bbox: 32633,
    )
    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.save_xarray",
        mock_save_xarray,
    )

    acquire_s2_pc(
        bbox=(13, 52, 14, 53),
        items=[item],
        save_dir=str(tmp_path),
        basename="test",
        chunks={"x": 64, "y": 128},
    )

    assert load_kwargs["encoding"] == {"B04": {"chunks": (64, 128)}}

def test_acquire_items_s2(monkeypatch):
    """Dispatch S2 acquisition to acquire_s2_pc."""
    expected = ("dataset", "test.zarr")

    monkeypatch.setattr(
        "eo_ml_pipeline.acquisition.stac.acquire_s2_pc",
        lambda **kwargs: expected,
    )

    result = acquire_items(
        "S2",
        bbox=(13, 52, 14, 53),
        items=[],
        save_dir="./",
        basename="test",
    )

    assert result == expected

def test_acquire_items_unsupported(caplog):
    """Return None for unsupported platforms."""
    result = acquire_items("L8")

    assert result is None
    assert "L8 is not available yet" in caplog.text

