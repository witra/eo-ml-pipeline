import datetime
import logging
import os
from datetime import datetime, timedelta

import geopandas as gpd
import pystac
import pystac_client

from eo_ml_pipeline.utils.geom import bbox_to_epsg

logger = logging.getLogger(__name__)

def temporal_sample_item(items:list[pystac.Item], 
                         start:datetime, 
                         end:datetime, 
                         interval_day:int=7,
                         sampling_fn=None # TODO: come up with a better sampling method
                         ):
    """Sample STAC items from consecutive temporal windows.

    The interval is divided into windows of ``interval_day`` days. At most
    one item is selected from each non-empty window. If ``sampling_fn`` is
    provided, it is called with the candidate items; otherwise, the first
    candidate is selected.

    Parameters
    ----------
    items : list[pystac.Item]
        STAC items to sample. Each item must have a ``datetime`` value.
    start : datetime
        Start of the sampling period (inclusive).
    end : datetime
        End of the sampling period (exclusive).
    interval_day : int, default=7
        Length of each temporal sampling window in days.
    sampling_fn : callable, optional
        Function used to select one item from each non-empty window.
        It receives a list of candidate items and should return one item.

    Returns
    -------
    list[pystac.Item]
        Selected items, with at most one item per temporal window.
    """
    selected = []
    current = start
    while current < end:
        window_end = min(current + timedelta(days=interval_day), end)
        candidates = [
            item
            for item in items 
            if current <= item.datetime < window_end
        ]

        if len(candidates) > 0:
            if sampling_fn:
                sampled = sampling_fn(candidates)
            else:
                sampled = candidates[0]

            selected.append(sampled)
            
        current = window_end
    return selected


     
def search_s2(catalog="https://planetarycomputer.microsoft.com/api/stac/v1",
              collection="sentinel-2-l2a",
              save_dir=None, 
              basename=None,
              **kwargs):
    """Search the Sentinel-2 STAC collection.

    Searches the specified STAC catalog and collection using the supplied
    spatial, temporal, and query parameters. When ``save_dir`` and
    ``basename`` are provided, the search results are additionally saved
    as a CSV file.

    Parameters
    ----------
    catalog : str, default=Planetary Computer STAC API
        STAC catalog URL.
    collection : str, default="sentinel-2-l2a"
        STAC collection to search.
    save_dir : str, optional
        Directory where the search results CSV is saved.
    basename : str, optional
        Output filename without the ``.csv`` extension.
    **kwargs
        Search parameters. Must include ``bbox``, ``datetime``, ``query``,
        and ``max_items``.

    Returns
    -------
    pystac.ItemCollection
        STAC items returned by the search.
    """

    kwargs_search = {
        "bbox": kwargs['bbox'],
        "datetime": kwargs['datetime'],
        "query": kwargs['query'],
        "max_items": kwargs['max_items'],
        }
    proj_epsg =  bbox_to_epsg(*kwargs['bbox'])       
    catalog_pystac = pystac_client.Client.open(catalog)
    search = catalog_pystac.search(collections=[collection], **kwargs_search)
    items = search.item_collection()
    logger.info(f"number of S2 scenes found on {kwargs['bbox']}: {len(items)}")
    if len(items) > 0 and save_dir and basename:
        if save_dir.startswith("s3://"):
                pass # TODO save to s3 bucket
        else: 
            child_dir = basename.rsplit("/", 1)[0] if "/" in basename else ""
            os.makedirs(f"{save_dir}/{child_dir}", exist_ok=True)
        df = gpd.GeoDataFrame.from_features(items.to_dict(), crs=f"epsg:{proj_epsg}")
        df.to_csv(f'{save_dir}/{basename}.csv')
    return items

def search_items(platform, **kwargs):
    """
    Dispatch a search request to the platform-specific implementation.

    Currently, platforms containing ``"S2"`` are handled by
    :func:`search_s2`. Unsupported platforms produce a warning and return
    ``None``.

    Parameters
    ----------
    platform : str
        Platform identifier.
    **kwargs
        Arguments forwarded to the platform-specific search function.

    Returns
    -------
    pystac.ItemCollection or None
        Search results for a supported platform, or ``None`` when the
        platform is not currently supported.
    """
    if 'S2' in platform:
        return search_s2(**kwargs)
    else: 
        logger.warning(f'The {platform} is not available yet. Please check the available platforms')
