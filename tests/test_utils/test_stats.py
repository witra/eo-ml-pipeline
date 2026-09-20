import json
import numpy as np
import pytest
import xarray as xr

from eo_ml_pipeline.utils.stats import calculate_mean_std

@pytest.fixture
def zarr_datasets(tmp_path):
    """Create small Zarr datasets for testing."""

    ds1 = xr.Dataset(
            {
                "var": (
                    ("y", "x"),
                    np.array([
                        [1.0, 2.0],
                        [3.0, np.nan],
                    ]),
                ),
            }
        )
    
    ds2 = xr.Dataset(
        {
            "var": (
                ("y", "x"),
                np.array([
                    [5.0, 6.0],
                    [np.nan, 8.0],
                ]),
            ),
        }
    )


    path1 = tmp_path / "data_1.zarr"
    path2 = tmp_path / "data_2.zarr"

    ds1.to_zarr(path1)
    ds2.to_zarr(path2)

    return [str(path1), str(path2)]

def test_calculate_mean_std_without_previous_data(zarr_datasets, tmp_path):
    """Calculate global statistics from multiple Zarr datasets."""

    outpath = tmp_path / "mean_std.json"
    result = calculate_mean_std(
        paths=zarr_datasets,
        vars=["var"],
        outpath=str(outpath),
    )
    assert result == str(outpath)
    with open(outpath) as f:
        stats = json.load(f)

    # Valid values
    values = np.array([1, 2, 3, 5, 6, 8])

    expected_sum = values.sum()
    expected_sq = (values**2).sum()
    expected_n = len(values)
    expected_mean = values.mean()
    expected_std = values.std()

    assert stats["var"]["total_sum"] == expected_sum
    assert stats["var"]["total_sq"] == expected_sq
    assert stats["var"]["total_n"] == expected_n
    assert stats["var"]["mean"] == pytest.approx(expected_mean)
    assert stats["var"]["std"] == pytest.approx(expected_std)

def test_calculate_mean_std_with_previous_data(zarr_datasets, tmp_path):
    """Calculate global statistics from multiple Zarr datasets."""
    previous_values = np.array([10.0, 20.0, 30])

    prev_compute = {
        "var": {
            "total_sum": previous_values.sum(),
            "total_sq": (previous_values**2).sum(),
            "total_n": len(previous_values),
        }
    }
    outpath = tmp_path / "mean_std.json"
    result = calculate_mean_std(
        paths=zarr_datasets,
        vars=["var"],
        prev_compute=prev_compute,
        outpath=str(outpath),
    )
    assert result == str(outpath)
    with open(outpath) as f:
        stats = json.load(f)

    # Valid values
    values = np.array([10, 20, 30, 1, 2, 3, 5, 6, 8])

    expected_sum = values.sum()
    expected_sq = (values**2).sum()
    expected_n = len(values)
    expected_mean = values.mean()
    expected_std = values.std()

    assert stats["var"]["total_sum"] == expected_sum
    assert stats["var"]["total_sq"] == expected_sq
    assert stats["var"]["total_n"] == expected_n
    assert stats["var"]["mean"] == pytest.approx(expected_mean)
    assert stats["var"]["std"] == pytest.approx(expected_std)



