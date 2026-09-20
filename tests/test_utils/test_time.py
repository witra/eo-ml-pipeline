import pytest

from eo_ml_pipeline.utils.time import buffer_date

@pytest.mark.parametrize(
    ("date", "buffer", "mode", "expected"),
    [
        # Left buffer
        ("20-09-2026", 7, "left", "2026-09-13/2026-09-20"),

        # Right buffer
        ("20-09-2026", 7, "right", "2026-09-20/2026-09-27"),

        # Center buffer - even
        ("20-09-2026", 6, "center", "2026-09-17/2026-09-23"),

        # Center buffer - odd: extra day goes to the right
        ("20-09-2026", 7, "center", "2026-09-17/2026-09-24"),

        # Zero buffer
        ("20-09-2026", 0, "left", "2026-09-20/2026-09-20"),
        ("20-09-2026", 0, "right", "2026-09-20/2026-09-20"),
        ("20-09-2026", 0, "center", "2026-09-20/2026-09-20"),

        # Month boundary
        ("01-10-2026", 3, "left", "2026-09-28/2026-10-01"),
        ("30-09-2026", 3, "right", "2026-09-30/2026-10-03"),

        # Year boundary
        ("01-01-2026", 3, "left", "2025-12-29/2026-01-01"),
        ("30-12-2026", 3, "right", "2026-12-30/2027-01-02"),
    ]
)
def test_buffer_date(date, buffer, mode, expected):
    assert buffer_date(date, buffer, mode) == expected

@pytest.mark.parametrize("mode", ["invalid", "", "LEFT", "rightward"])
def test_buffer_date_invalid_mode(mode):
    with pytest.raises(
        ValueError,
        match="mode must be 'left', 'right', or 'center'",
    ):
        buffer_date("20-09-2026", 7, mode)

@pytest.mark.parametrize(
    "date",
    [
        "2026-09-20",  # Wrong format
        "20/09/2026",  # Wrong separator
        "20-09-26",    # Wrong year format
        "not-a-date",  # Invalid date
        "31-02-2026",  # Impossible date
    ],
)
def test_buffer_date_invalid_date(date):
    with pytest.raises(ValueError):
        buffer_date(date, 7)