import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def buffer_date(date: str, buffer:int, mode='center'):
    date = datetime.strptime(date, "%d-%m-%Y").replace(tzinfo=datetime.UTC)
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