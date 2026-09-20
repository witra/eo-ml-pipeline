import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def buffer_date(date: str, buffer:int, mode='center'):
    """Create a buffered date interval around a reference date.

    Creates a date interval by extending the reference date according to
    the specified buffering mode. The input date must be provided in
    ``DD-MM-YYYY`` format, while the returned interval uses
    ``YYYY-MM-DD/YYYY-MM-DD`` format.

    Parameters
    ----------
    date : str
        Reference date in ``DD-MM-YYYY`` format.
    buffer : int
        Number of days by which the reference date is buffered.
    mode : {"left", "right", "center"}, default="center"
        Direction in which the date is buffered.

        - ``"left"`` : Extend the interval ``buffer`` days before the
          reference date.
        - ``"right"`` : Extend the interval ``buffer`` days after the
          reference date.
        - ``"center"`` : Distribute the buffer around the reference date.
          When ``buffer`` is odd, the additional day is placed on the
          right side.

    Returns
    -------
    str
        Date interval in ``YYYY-MM-DD/YYYY-MM-DD`` format.

    Raises
    ------
    ValueError
        If ``mode`` is not one of ``"left"``, ``"right"``, or ``"center"``.
        Also raised if ``date`` does not follow the expected date format
        or represents an invalid date.

    Examples
    --------
    >>> buffer_date("20-09-2026", 7, mode="left")
    '2026-09-13/2026-09-20'

    >>> buffer_date("20-09-2026", 7, mode="right")
    '2026-09-20/2026-09-27'

    >>> buffer_date("20-09-2026", 6, mode="center")
    '2026-09-17/2026-09-23'

    >>> buffer_date("20-09-2026", 7, mode="center")
    '2026-09-17/2026-09-24'
    """
    date = datetime.strptime(date, "%d-%m-%Y")
    if mode == "left":
        start = date - timedelta(days=buffer)
        end = date
    elif mode == "right":
        start = date
        end = date + timedelta(days=buffer)
    elif mode == "center":
        left = buffer // 2
        right = buffer - left 
        start = date - timedelta(days=left)
        end = date + timedelta(days=right)
    else:
        raise ValueError("mode must be 'left', 'right', or 'center'")
    return (
        f"{start.strftime('%Y-%m-%d')}/"
        f"{end.strftime('%Y-%m-%d')}"
        )