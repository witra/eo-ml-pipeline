from datetime import datetime, timedelta

import pystac
import pytest

from eo_ml_pipeline.discovery.stac import search_items, search_s2, temporal_sample_item


def make_item(dt):
    """Create a minimal STAC item for testing."""
    return pystac.Item(
        id=dt.strftime("%Y%m%d"),
        geometry=None,
        bbox=None,
        datetime=dt,
        properties={},
    )

def test_temporal_sample_item():
    """Select at most one item from each temporal window."""

    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 15)

    items = [
        make_item(datetime(2026, 1, 2)),
        make_item(datetime(2026, 1, 4)),
        make_item(datetime(2026, 1, 8)),
        make_item(datetime(2026, 1, 12)),
    ]

    result = temporal_sample_item(
        items,
        start,
        end,
        interval_day=7,
    )

    assert [item.datetime for item in result] == [
        datetime(2026, 1, 2),
        datetime(2026, 1, 8),
    ]

def test_temporal_sample_item_sampling_fn():
    """Use the sampling function to select an item from each window."""

    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 15)

    items = [
        make_item(datetime(2026, 1, 2)),
        make_item(datetime(2026, 1, 4)),
        make_item(datetime(2026, 1, 8)),
    ]

    result = temporal_sample_item(
        items,
        start,
        end,
        interval_day=7,
        sampling_fn=lambda candidates: candidates[-1],
    )

    assert [item.datetime for item in result] == [
        datetime(2026, 1, 4),
        datetime(2026, 1, 8),
    ]

def test_temporal_sample_item_empty_window():
    """Skip temporal windows without candidates."""

    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 15)

    items = [
        make_item(datetime(2026, 1, 10)),
    ]

    result = temporal_sample_item(
        items,
        start,
        end,
        interval_day=7,
    )

    assert len(result) == 1
    assert result[0].datetime == datetime(2026, 1, 10)

def test_search_s2(monkeypatch, tmp_path):
    """Search Sentinel-2 items and save the results."""
    items = [make_item(datetime(2026, 1, 2))]

    class MockSearch:
        def item_collection(self):
            return pystac.ItemCollection(items)

    class MockCatalog:
        def search(self, **kwargs):
            assert kwargs["collections"] == ["sentinel-2-l2a"]
            assert kwargs["bbox"] == [13, 52, 14, 53]
            assert kwargs["datetime"] == "2026-01-01/2026-01-15"
            assert kwargs["max_items"] == 10
            return MockSearch()

    monkeypatch.setattr(
        "eo_ml_pipeline.discovery.stac.pystac_client.Client.open",
        lambda catalog: MockCatalog(),
    )
    monkeypatch.setattr(
        "eo_ml_pipeline.discovery.stac.bbox_to_epsg",
        lambda *bbox: 32633,
    )

    result = search_s2(
        bbox=[13, 52, 14, 53],
        datetime="2026-01-01/2026-01-15",
        query={},
        max_items=10,
        save_dir=str(tmp_path),
        basename="test",
    )

    assert len(result) == 1
    assert result[0].id == "20260102"
    assert (tmp_path / "test.csv").exists()

def test_search_items_s2(monkeypatch):
    """Dispatch S2 searches to search_s2."""

    expected = ["item"]

    monkeypatch.setattr(
        "eo_ml_pipeline.discovery.stac.search_s2",
        lambda **kwargs: expected,
    )

    result = search_items("S2", bbox=[13, 52, 14, 53])

    assert result == expected

def test_search_items_unsupported(caplog):
    """Return None for unsupported platforms."""

    result = search_items("L8")

    assert result is None
    assert "L8 is not available yet" in caplog.text