import json
import logging
import os
import time
from glob import glob

import xarray as xr
from tqdm import tqdm

from eo_pipeline.acquisition.stac import acquire_items
from eo_pipeline.dataset.base import construct_xy
from eo_pipeline.discovery.stac import search_items
from eo_pipeline.processing.preprocessing import apply_preprocessing
from eo_pipeline.utils.geom import get_bbox_from_tif
from eo_pipeline.utils.stats import calculate_mean_std

logging.basicConfig(
    level=logging.INFO,
     format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

def base_pipeline(y_dir, xkwargs, record:str | dict | None=None, max_retries=3):
    """
    y_dir: file.tif
    """
    if isinstance(record, str):
        with open("records", "r") as f:
            record = json.load(f)
    elif isinstance(record, dict):
        record = record
    else:
        logger.warning("rocord is not avaialbale/recognised. Initialized instead")
        record = {}
            
    y_paths = glob(os.path.join(y_dir, "*.tif"), recursive=True)
    logger.info(f"There are {len(y_paths)} tif files found")
    record = {}
    for x_name, kwargs in xkwargs.items():
        preprocessed_paths = []
        record[x_name] = []
        failed_items_dict = {}
        for y_path in tqdm(y_paths[:2]):
            # Setup
            # Assumption: each basename has prefix at the first word
            basename = os.path.basename(y_path).split('.')[0].split('_')[1:]
            basename = "_".join(basename)
            if basename  in record[x_name]:
                continue
            kwargs['basename'] = basename 
            logger.info(f"process data for {basename}")

            bbox = get_bbox_from_tif(y_path)
            kwargs['bbox'] = bbox

            # search stac items
            items = search_items(x_name, **kwargs)
            kwargs['all_items'] = items

            if len(items)==0:
                logger.warning(f'there is no item found on {basename}')
                continue

            # acquire items
            x_dss = []
            x_paths = []
            failed_items = [] 
            for i, item in enumerate(items):
                for attempt in range(max_retries):
                    try:
                        kwargs['basename'] = f'{x_name}/images/{x_name}_{basename}/item{i}_{item.id}'
                        kwargs['items'] = [item, ]
                        x_ds, x_path = acquire_items(x_name, **kwargs)
                        x_dss.append(x_ds), x_paths.append(x_path)
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            failed_items.append(item)
                        print(f"Attempt {attempt + 1} failed: {e}")
                        time.sleep(5)
            if len(x_paths)==0:
                raise ValueError('there is no success item acquired')
            ds_ref = x_dss[0]
            for i, ds in enumerate(x_dss[1:], start=1):
                if not ds_ref.x.equals(ds.x):
                    logger.warning(f"Dataset {i}: x coordinates differ")
                if not ds_ref.y.equals(ds.y):
                    logger.warning(f"Dataset {i}: y coordinates differ")
            kwargs['ds'] = xr.concat(x_dss, dim="time")

            # preprocessing
            if kwargs.get("save_preprocessed", None):
                kwargs['basename'] = f'{x_name}/preprocess/{x_name}_preprocessed_{basename}'
            x_ds, preprocessed_path  = apply_preprocessing(x_name, **kwargs)
            preprocessed_paths.append(preprocessed_path)

            # construct x and label dataset
            kwargs['basename'] = f'{x_name}/xypair/{x_name}_xypair_{basename}'
            ds, _ = construct_xy(x_ds, y_path, **kwargs)
            record[x_name].append(basename)
            failed_items_dict[basename] = failed_items
            del ds
            # break

        with open(os.path.join(f"{kwargs['save_dir']}", f"{x_name}_succes_records.json"), "w") as f:
            json.dump(record[x_name], f, indent=4)
        with open(os.path.join(f"{kwargs['save_dir']}", f"{x_name}_failed_records.json"), "w") as f:
            json.dump(failed_items_dict, f, indent=4)
        
        # cal global stat to each satellite data
        kwargs['outpath'] = os.path.join(kwargs["save_dir"], f'{x_name}_mean_std.json')
        calculate_mean_std(preprocessed_paths, kwargs["bands"][1:], **kwargs)

if __name__ == '__main__':
    xkwargs = {
        "S2":{
            "datetime": "2020-10-01/2021-01-01",
            "bands": [
                        "SCL", #"B01", "B02", "B03",
                        # "B04", "B05", "B06", "B07",     
                        # "B08", "B8A", "B09", "B11", 
                        "B12",
                     ] ,
            "save_dir": "./dataset/",
            "query": {"eo:cloud_cover": {"lte": 20}},
            "max_items": 2,
            "chunks": {"x": 256, "y": 256, "time": 1},
            "resolution":10,
            "is_save_tif": True,
            "dtype": "uint16",
            "nodata": 0,
            "save_preprocessed": True,
            }
    }
    y_dir = '../../ModularGeoFM_dataprep/dataset/AI4LCC_CNRS/GE/GT/line_filled'
    base_pipeline(y_dir, xkwargs)