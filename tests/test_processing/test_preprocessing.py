import numpy as np
import xarray as xr

from eo_ml_pipeline.processing.preprocessing import (
    apply_cloud_mask_s2,
    calculate_median_composite,
    scale_reflectance_s2,
    apply_preprocessing_s2_base
)


def test_apply_cloud_mask_s2():
    """Mask selected SCL classes."""

    ds = xr.Dataset(
        {
            "B04": (("y", "x"), [[100, 200], [300, 400]]),
            "SCL": (("y", "x"), [[4, 8], [3, 5]]),
        }
    )

    result = apply_cloud_mask_s2(ds, [3, 8])

    assert np.isnan(result.B04.values[0, 1])
    assert np.isnan(result.B04.values[1, 0])
    assert result.B04.values[0, 0] == 100
    assert result.B04.values[1, 1] == 400
    assert "SCL" not in result


def test_calculate_median_composite():
    """Calculate the median across time."""

    ds = xr.Dataset(
        {
            "B04": (
                ("time", "y", "x"),
                [
                    [[100, 200]],
                    [[200, 400]],
                    [[300, 600]],
                ],
            )
        }
    )

    result = calculate_median_composite(ds)

    assert result.B04.values.tolist() == [[200, 400]]


def test_scale_reflectance_s2():
    """Scale Sentinel-2 reflectance values."""

    ds = xr.Dataset(
        {"B04": (("y", "x"), [[1000, 5000]])}
    )

    result = scale_reflectance_s2(ds)

    np.testing.assert_allclose(
        result.B04.values,
        [[0.1, 0.5]],
    )

def test_apply_preprocessing_s2_base():
    """Apply the complete Sentinel-2 preprocessing workflow."""

    ds = xr.Dataset(
        {
            "B04": (
                ("time", "y", "x"),
                [
                    [[1000, 2000]],
                    [[2000, 4000]],
                ],
            ),
            "SCL": (
                ("time", "y", "x"),
                [
                    [[4, 8]],
                    [[4, 4]],
                ],
            ),
        }
    )

    result, zarr_path = apply_preprocessing_s2_base(ds, [4, ])

    np.testing.assert_allclose(
        result.B04.values,
        [[np.nan, 0.2]],
    )

    assert "SCL" not in result
    assert zarr_path is None