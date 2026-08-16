import logging
import pystac_client

logger = logging.getLogger(__name__)

def search_s2(bbox, 
              datetime, 
              catalog="https://planetarycomputer.microsoft.com/api/stac/v1",
              collection="sentinel-2-l2a",
              max_cloud_cover=20, 
              max_item=10,
              kwargs_search=None):

    if not kwargs_search:
        kwargs_search = dict(
            bbox=bbox,
            datetime=datetime,
            query={"eo:cloud_cover": {"lte": max_cloud_cover}},
            max_items=max_item
            )       
    catalog_pystac = pystac_client.Client.open(catalog)
    search = catalog_pystac.search(collections=[collection], **kwargs_search)
    items = search.item_collection()
    logger.info(f"number of S2 scenes found of {bbox}: {len(items)}")
    return items
