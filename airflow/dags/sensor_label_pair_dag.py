import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from glob import glob
from pathlib import Path

import pystac
import xarray as xr
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.sdk import Param, dag, task, task_group
from annotated_types import Unit
from zarr import config

from eo_ml_pipeline.acquisition.stac import acquire_items
from eo_ml_pipeline.dataset.base import construct_xy
from eo_ml_pipeline.discovery.stac import search_items, temporal_sample_item
from eo_ml_pipeline.processing.preprocessing import apply_preprocessing
from eo_ml_pipeline.utils.geom import bbox_to_epsg, get_bbox_from_tif
from eo_ml_pipeline.utils.io import save_xarray

logger = logging.getLogger(__name__)

def _get_start_end_date(date_range:str, timezone:timezone):
    start_date, end_date = (
        datetime.fromisoformat(date).replace(tzinfo=timezone)
        for date in date_range.split("/")
    )
    return start_date, end_date

def _get_unit_info(unit: dict):
    y_path = unit["y_path"]
    sensor = unit["sensor"]
    sensor_params = unit["sensor_params"]
    y_name = os.path.basename(y_path).split('.')[0].split('_')[1:]
    y_name = "_".join(y_name)
    return y_name, y_path, sensor, sensor_params

@task
def get_y_files() -> list[str]:
    context = get_current_context()
    y_dir = context["params"]["y_dir"]
    paths = glob(f"{y_dir}/*.tif")[:2]
    logger.info(os.getcwd())
    logger.info(f"Found {len(paths)} y files in {y_dir}")
    return paths

@task
def build_work_unit(y_paths: list[str]) -> list[dict]:
    params = get_current_context()["params"]
    sensors = params["sensors"]
    logger.info(f"sensors: {sensors}")
    work_configs  = []
    for i, y_path in enumerate(y_paths):
        for sensor_name, sensor_dict in sensors.items():
            work_unit = {
                        "work_id": f"work_{i}_{sensor_name}",
                        "y_path": y_path,
                        "sensor": sensor_name,
                        "sensor_params": sensor_dict
                        }
            work_configs.append(work_unit)
            logger.info(work_unit)
    logger.info(f"Total work units: {len(work_configs)}")
    return work_configs

@task
def search_stac_items(unit: dict) -> list[dict]:
    y_name, y_path, sensor, sensor_params = _get_unit_info(unit)
    bbox = get_bbox_from_tif(y_path)
    sensor_params["bbox"] = bbox
    items = search_items(sensor, **sensor_params)
    start_date, end_date = _get_start_end_date(sensor_params["datetime"], timezone.utc)
    logger.info(f"{start_date}, {end_date}, {len(items)}")
    items = temporal_sample_item(items, start_date, end_date, interval_day=7)
    if len(items)==0:
        raise AirflowSkipException(f"Skipping work unit of {y_name} with sensor {sensor} due to no items found")
    else:
        items = [item.to_dict()for item in items]
        logger.info(f"{start_date}, {end_date}, {len(items)}")
        items_path = Path(sensor_params["save_dir"]) / f"items/{sensor}/{y_name}.json"
        items_path.parent.mkdir(parents=True, exist_ok=True)
        items_path.write_text(json.dumps(items, indent=4))
        sensor_params["items_path"] = str(items_path)
        sensor_params["search_status"] = "success"
        logger.info(f"{len(items)} items are saving to {items_path}")
    return unit

@task(retries=3,  
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=False,)
def acquire_stac_items(unit: dict) -> list[dict]:
    y_name, _, sensor, sensor_params = _get_unit_info(unit)
    items_path = Path(sensor_params["items_path"])
    items = [pystac.Item.from_dict(item) for item in json.loads(items_path.read_text())]
    completed_items_path = Path(f"{sensor_params['save_dir']}/interim/images/{sensor}/{sensor}_{y_name}/completed_items.json")
    completed_items_path.parent.mkdir(parents=True, exist_ok=True)
    if completed_items_path.exists():
        completed_items_list = json.loads(completed_items_path.read_text())
        logger.info(f"completed list: {completed_items_list}")
        items = [item for item in items if item.id not in completed_items_list]
    else:
        completed_items_list = []
    for item in items:
        basename = f"interim/images/{sensor}/{sensor}_{y_name}/{item.id}"
        try:
            _, _ = acquire_items(sensor, items=[item,], basename=basename, **sensor_params)
            completed_items_list.append(item.id)
            completed_items_path.write_text(json.dumps(completed_items_list, indent=4))
        except Exception as e:
            logger.error(f"Error occurred while acquiring item {item.id}: {e}")
            raise
    zarr_paths = [f"{sensor_params['save_dir']}/interim/images/{sensor}/{sensor}_{y_name}/{item_id}.zarr" for item_id in completed_items_list]
    sensor_params["zarr_paths"] = zarr_paths
    logger.info(f"{sensor_params}")
    return unit

@task
def concat_zarr(unit: dict) -> list[dict]:
    y_name, _, sensor, sensor_params = _get_unit_info(unit)
    zarr_paths = sensor_params["zarr_paths"]
    logger.info(f"{zarr_paths}")
    if len(zarr_paths)==0:
        raise AirflowSkipException(f"Skipping work unit of {unit['y_path']} with sensor {unit['sensor']} due to no zarr files found")
    else:
        ds_list = []
        proj_epsg =  bbox_to_epsg(*sensor_params["bbox"])
        for i, zarr_path in enumerate(zarr_paths):
            ds = xr.open_zarr(zarr_path, consolidated=False)
            ds = ds.rio.write_crs(f"epsg:{proj_epsg}", inplace=True)
            if i==0: # ds_list [0] is the ref
                ds_list.append(ds)
            else:
                ds = ds.rio.reproject_match(ds_list[0])
                if not ds_list[0].x.equals(ds.x):
                    logger.warning(f"Dataset {i}: x coordinates differ")
                if not ds_list[0].y.equals(ds.y):
                    logger.warning(f"Dataset {i}: y coordinates differ")
                ds_list.append(ds)
        ds_concat = xr.concat(ds_list, dim="time")
        basename  = f"interim/images/{sensor}/{sensor}_{y_name}"
        sensor_params["encoding"] = {var: {"chunks": (sensor_params["chunks"]["x"], sensor_params["chunks"]["y"])} 
                                                        for var in ds_concat.data_vars 
                                                        if "x" in ds_concat[var].dims and "y" in ds_concat[var].dims}
        zarr_path = save_xarray(ds_concat, filename=basename, save_format='zarr', **sensor_params)
        sensor_params["zarr_path"] = zarr_path
        logger.info(f"Saved concatenated zarr to {zarr_path}")
    return unit

@task
def preproces(unit: dict) -> list[dict]:
    y_name, _, sensor, sensor_params = _get_unit_info(unit)
    basename  = f"interim/images/{sensor}_preprocess/{sensor}_{y_name}"
    ds = xr.open_zarr(sensor_params["zarr_path"], consolidated=False)
    ds = ds.chunk({"x":sensor_params["chunks"]["x"],
                    "y":sensor_params["chunks"]["y"]})
    sensor_params["encoding"] = {var: {"chunks": (sensor_params["chunks"]["x"], sensor_params["chunks"]["y"])} 
                                    for var in ds.data_vars 
                                    if "x" in ds[var].dims and "y" in ds[var].dims}
    _, preprocessed_path  = apply_preprocessing(sensor, ds=ds, basename=basename, **sensor_params)
    sensor_params["preprocessed_path"] = preprocessed_path
    logger.info(f"Saved preprocessed zarr to {preprocessed_path}")
    return unit

@task
def construct_sensor_label_dataset(unit: dict):
    y_name, y_path, sensor, sensor_params = _get_unit_info(unit)
    basename  = f"final/images/{sensor}_xy_pair/{sensor}_{y_name}"
    proj_epsg =  bbox_to_epsg(*sensor_params["bbox"])
    sensor_params["chunks"].pop("time")
    x_ds = xr.open_zarr(sensor_params["preprocessed_path"], consolidated=False)
    x_ds = x_ds.rio.write_crs(f"epsg:{proj_epsg}", inplace=True)
    x_ds = x_ds.chunk({"x":sensor_params["chunks"]["x"],
                        "y":sensor_params["chunks"]["y"]})
    sensor_params["encoding"] = {var: {"chunks": (sensor_params["chunks"]["x"], sensor_params["chunks"]["y"])} 
                                            for var in x_ds.data_vars 
                                            if "x" in x_ds[var].dims and "y" in x_ds[var].dims}
    _, xy_pair_path = construct_xy(x_ds, y_path, basename=basename, **sensor_params)
    sensor_params["xy_pair_path"] = xy_pair_path
    logger.info(f"Saved xy pair zarr to {xy_pair_path}")
    return unit

@task_group
def process_aoi(unit:dict): 
    unit = search_stac_items(unit)
    unit = acquire_stac_items(unit)
    unit = concat_zarr(unit)
    unit = preproces(unit)
    unit = construct_sensor_label_dataset(unit)

@dag(
    dag_id="sensor_label_pair",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "y_dir": Param("./data/labels", type="string"),
        "sensors":Param({
                        "S2":{
                        "datetime": "2020-01-01/2021-01-01",
                        "bands": [
                                    "SCL", "B01", "B02", "B03",
                                    "B04", "B05", "B06", "B07",     
                                    "B08", "B8A", "B09", "B11", 
                                    "B12",
                                ] ,
                        "save_dir": "./data/",
                        "query": {"eo:cloud_cover": {"lte": 20}},
                        "max_items": 10,
                        "chunks": {"x": 256, "y": 256, "time": 1},
                        "resolution":10,
                        "is_save_tif": True,
                        "dtype": "uint16",
                        "nodata": 0,
                        "save_preprocessed": True}
                        }
                    ,type="object", items={"type":"object"})
        
    }
)
def xy_builder():
    # Tasks: 
    # 1. Get the list of y files from y dir
    # 2. Identify each sensor types and their corresponding paramteres
    # 3. One work package consist of one y file and one sensor type. Hence, total number of works are num of y files * number of sensor
    # 4. Work package consisting of the following steps:
    # 4a. Search the available stac items
    # 4b. Download the all available stac items (handling the failed items here)
    # 4c. Preprocess the downloaded items
    # 4d. construct the x and y pair dataset
    # 5. Calculate the global mean and std for each sensor type

    work_units = build_work_unit(get_y_files())
    process_aoi.expand(unit=work_units)
xy_builder()

