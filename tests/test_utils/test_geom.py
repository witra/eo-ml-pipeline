import numpy as np
import rasterio
from rasterio.transform import from_origin

from eo_ml_pipeline.utils.geom import bbox_to_epsg, get_bbox, get_bbox_from_tif


def test_get_bbox():
    """Create a bounding box around a point."""

    bbox = get_bbox(lon=13.405, lat=52.52, half_size_m=1000)

    assert len(bbox) == 4
    assert bbox[0] < bbox[2]
    assert bbox[1] < bbox[3]
    assert bbox[0] < 13.405 < bbox[2]
    assert bbox[1] < 52.52 < bbox[3]

def test_bbox_to_epsg():
    """Return the UTM EPSG code for a bounding box."""

    epsg = bbox_to_epsg(
        min_lon=13.39,
        min_lat=52.51,
        max_lon=13.42,
        max_lat=52.53,
    )
    print(type(epsg), epsg)
    assert epsg == '32633'

def test_get_bbox_from_tif(tmp_path):
    """Return the WGS 84 bounding box from a GeoTIFF."""

    path = tmp_path / "test.tif"

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(13.0, 53.0, 0.1, 0.1),
    ) as dst:
        dst.write(np.ones((1, 2, 2), dtype="float32"))

    bbox = get_bbox_from_tif(path)

    assert bbox == [13.0, 52.8, 13.2, 53.0]