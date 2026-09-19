import datetime
import logging
import os
from datetime import datetime, timedelta

import geopandas as gpd
import pystac
import pystac_client

from eo_pipeline.utils.geom import bbox_to_epsg

logger = logging.getLogger(__name__)

def temporal_sample_item(items:list[pystac.Item], 
                         start:datetime, 
                         end:datetime, 
                         interval_day:int=7,
                         sampling_fn=None # TODO: come up with a better sampling method
                         ):
    selected = []
    current = start
    while current < end:
        window_end = current + timedelta(days=interval_day)
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
    """
    datetime: range from/to "yyyy-mm-dd/yyyy-mm-dd"
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
    logger.info(f"number of S2 scenes found of {kwargs['bbox']}: {len(items)}")
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
    if 'S2' in platform:
        return search_s2(**kwargs)
    else: 
        logger.warning(f'The {platform} is not available yet. Please check the available platforms')
