import logging
import os
import rioxarray as rio
import planetary_computer as pc
from odc.stac import stac_load

from eo_pipeline.utils.geom import bbox_to_epsg
from eo_pipeline.utils.io import save_xarray
from eo_pipeline.utils.path import delete_path

logger = logging.getLogger(__name__)

def acquire_s2_pc(bbox,
                  items,  
                  save_dir, 
                  basename,
                  **kwargs):
    proj_epsg =  bbox_to_epsg(*bbox)
    kwargs["crs"] = f"epsg:{proj_epsg}"
    added_keys = ["crs", ]
    if not kwargs.get('bands'):
        kwargs['bands'] = [
                            "SCL", "B01", "B02", "B03",
                            "B04", "B05", "B06", "B07",     
                            "B08", "B8A", "B09", "B11", "B12",
                            ]     
    if len(items)==0:
        logger.warning(f"No S2 scenes found for {basename}")
        return 0
    if save_dir.startswith("s3://"):
        pass # TODO save to s3 bucket
    else: 
        child_dir = basename.rsplit("/", 1)[0] if "/" in basename else ""
        os.makedirs(f"{save_dir}/{child_dir}", exist_ok=True)
        delete_path(f"{save_dir}/{basename}.zarr")
    logger.info(f'Downloading {items}')
    signed_items = [pc.sign(item) for item in items]
    ds = stac_load(signed_items, groupby='id', **kwargs)
    ds = ds.rio.write_crs(f"epsg:{proj_epsg}", inplace=True)
    if kwargs.get("chunks", None) and not kwargs.get("encoding", None):
        kwargs["encoding"] = {var: {"chunks": (kwargs["chunks"]["x"], kwargs["chunks"]["y"])} 
                                    for var in ds.data_vars 
                                    if "x" in ds[var].dims and "y" in ds[var].dims}
    zarr_path = save_xarray(ds, save_dir, basename, 'zarr', **kwargs)
    logger.info(f'Finished {items}')
    for key in added_keys:
        kwargs.pop(key)
    return ds, zarr_path

def acquire_items(platform, **kwargs):
    if 'S2' in platform:
        return acquire_s2_pc(**kwargs)
    else: 
        logger.warning(f'The {platform} is not available yet. Please check the available platforms')

