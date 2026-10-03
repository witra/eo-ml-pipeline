import json
import logging
import os
from datetime import UTC, datetime, timedelta, timezone
from functools import partial
from glob import glob
from pathlib import Path

import pandas as pd
import pystac
import rioxarray as rio
import xarray as xr
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.sdk import Param, dag, task, task_group

from eo_ml_pipeline.acquisition import acquire_items
from eo_ml_pipeline.dataset import construct_xy
from eo_ml_pipeline.discovery import search_items, temporal_sample_item
from eo_ml_pipeline.processing import calculate_median_composite
from eo_ml_pipeline.utils.geom import bbox_to_epsg, get_bbox_from_tif, save_xarray

logger = logging.getLogger(__name__)


def _review_temporal_availability(list_paths:list[str], sensor):
    """
    Summarize the temporal availability of input items.
    
    Parameters
    ----------
    list_paths : list[str]
        Paths to the input Zarr items.
    sensor : str
        Sensor identifier. Currently only ``"S2"`` is supported.

    Returns
    -------
    pandas.Series
        Number of available items per month, indexed by month-end.

    Raises
    ------
    Exception
        If the specified sensor is not supported.
    """
    dates = []
    if sensor=="S2":
        ids = [os.path.basename(path).split(".")[0] for path in list_paths]
        date_strs = [os.path.basename(path).split(".")[0].split("_")[2]for path in list_paths]
        dates = [datetime.strptime(date_str, "%Y%m%dT%H%M%S") for date_str in date_strs]
        count = _count_by_period(dates, freq="ME")
    else:
        raise Exception("the {sensor} is not available yet")
    logger.info(f"{len(list_paths)} items to calculate the median:")
    logger.info(ids)
    logger.info("Sampling summary: ")
    logger.info(f"{count}")
    return count
    
def _count_by_period(dates, freq="ME"):
    """
    Count dates grouped into a specified time frequency.
    
    Parameters
    ----------
    dates : array-like
        Dates or datetime-like values to count.
    freq : str, default="ME"
        Pandas resampling frequency used to group the dates.

    Returns
    -------
    pandas.Series
        Number of dates in each time period, indexed by the
        corresponding period-end timestamp.
    """
    dates = pd.to_datetime(dates)
    return (
        pd.Series(1, index=dates)
        .resample(freq)
        .sum()
        .astype(int)
    )

def _get_start_end_date(date_range:str, timezone:timezone):
    """
    Parse an ISO 8601 date range into timezone-aware datetimes.

    Parameters
    ----------
    date_range : str
        Date range in ``"start/end"`` format, where both dates are
        ISO 8601-compatible strings.

    timezone : datetime.timezone
        Timezone to assign to the parsed datetime objects.

    Returns
    -------
    start_date : datetime.datetime
        Start of the requested date range.

    end_date : datetime.datetime
        End of the requested date range.

    Examples
    --------
    >>> _get_start_end_date("2020-01-01/2021-01-01", timezone.utc)
    (datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2021, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))
    """
    start_date, end_date = (
        datetime.fromisoformat(date).replace(tzinfo=timezone)
        for date in date_range.split("/")
    )
    return start_date, end_date

def _get_unit_info(unit: dict):
    """
    Extract processing information from a sensor-label work unit.

    Parameters
    ----------
    unit : dict
        Work-unit configuration containing the label path, sensor name,
        and sensor-specific parameters. Expected keys are ``"y_path"``,
        ``"sensor"``, and ``"sensor_params"``.

    Returns
    -------
    y_name : str
        Label basename derived from the label filename.

    y_path : str
        Path to the label GeoTIFF.

    sensor : str
        Sensor name.

    sensor_params : dict
        Sensor-specific processing parameters.
    """
    y_path = unit["y_path"]
    sensor = unit["sensor"]
    sensor_params = unit["sensor_params"]
    task_params = unit["tasks"]
    y_name = os.path.basename(y_path).split('.')[0].split('_')[1:]
    y_name = "_".join(y_name)
    return y_name, y_path, sensor, sensor_params, task_params

def _sample_items(items, keyword):
    selected = []
    for item in items:
        if keyword in item.id:
            selected.append(item)
    return selected


@task
def get_y_files() -> list[str]:
    """
    Find label GeoTIFF files configured for the DAG run.

    The label directory is obtained from the Airflow ``y_dir`` parameter.

    Returns
    -------
    list of str
        Paths to the label GeoTIFF files found in the configured directory.
    """
    context = get_current_context()
    y_dir = context["params"]["y_dir"]
    paths = glob(f"{y_dir}/*.tif")
    logger.info(os.getcwd())
    logger.info(f"Found {len(paths)} y files in {y_dir}")
    logger.info(f"{paths}")
    return paths

@task
def build_work_unit(y_paths: list[str]) -> list[dict]:
    """
    Build sensor-label processing work units.

    A work unit represents one combination of a label file and a sensor.
    Therefore, the total number of work units is the number of label files
    multiplied by the number of configured sensors.

    Parameters
    ----------
    y_paths : list of str
        Paths to label GeoTIFF files.

    Returns
    -------
    list of dict
        Work-unit configurations. Each dictionary contains:

        - ``work_id`` : Unique identifier for the work unit.
        - ``y_path`` : Path to the label GeoTIFF.
        - ``sensor`` : Sensor name.
        - ``sensor_params`` : Sensor-specific processing parameters.
    """
    params = get_current_context()["params"]
    sensors = params["sensors"]
    tasks = params["tasks"]
    logger.info(f"sensors: {sensors}")
    work_configs  = []
    for i, y_path in enumerate(y_paths):
        for sensor_name, sensor_dict in sensors.items():
            work_unit = {
                        "work_id": f"work_{i}_{sensor_name}",
                        "y_path": y_path,
                        "sensor": sensor_name,
                        "sensor_params": sensor_dict,
                        "tasks": tasks
                        }
            work_configs.append(work_unit)
            logger.info(work_unit)
    logger.info(f"Total work units: {len(work_configs)}")
    return work_configs


@task
def search_stac_items(unit: dict) -> list[dict]:
    """
    Search and temporally sample STAC items for a processing unit.

    The label bounding box is used to spatially constrain the STAC search.
    Retrieved items are temporally sampled at seven-day intervals and saved
    as a JSON file for the subsequent acquisition task.

    Parameters
    ----------
    unit : dict
        Sensor-label work unit containing the label path, sensor name,
        and sensor parameters.

    Returns
    -------
    dict
        The updated work unit containing the path to the saved STAC item
        records and a successful search status.

    Raises
    ------
    AirflowSkipException
        If no STAC items are found for the work unit.

    Notes
    -----
    The STAC item metadata is stored under::

        <save_dir>/items/<sensor>/<label_name>.json
    """
    y_name, y_path, sensor, sensor_params, task_params = _get_unit_info(unit)
    granule = y_name.split("_")[1]
    bbox = get_bbox_from_tif(y_path)
    sensor_params["bbox"] = bbox
    interval_day = task_params["item_interval_day"]
    
    items = search_items(sensor, **sensor_params)
    logger.info(f"item ids found for granule {granule}: ")
    for i, item in enumerate(items):
        logger.info(f"{i}, {item.id}")
    start_date, end_date = _get_start_end_date(sensor_params["datetime"], UTC)
    items = temporal_sample_item(items, start_date, end_date, interval_day=interval_day, sampling_fn=partial(_sample_items, keyword=granule))
    
    logger.info(f"Search Date from {start_date} to {end_date}")
    logger.info(f"initial num of items: {len(items)}")
    logger.info(f"sampling interval in days: {interval_day}")
    logger.info(f"post sampling items: {len(items)}")
    for i, item in enumerate(items):
            logger.info(f"{i}, {item.id}")
    
    if len(items)==0:
        raise AirflowSkipException(f"Skipping work unit of {y_name} with sensor {sensor} due to no items found")
    else:
        items = [item.to_dict()for item in items]
        items_path = Path(sensor_params["save_dir"]) / f"items/{sensor}/{y_name}.json"
        items_path.parent.mkdir(parents=True, exist_ok=True)
        items_path.write_text(json.dumps(items, indent=4))
        sensor_params["items_path"] = str(items_path)
        sensor_params["search_status"] = "success"
        logger.info(f"{len(items)} items are saving to {items_path}")
    return unit

@task(retries=100,  
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=False,)
def acquire_stac_items(unit: dict) -> list[dict]:
    """
    Acquire the STAC items associated with a processing unit.

    STAC item metadata is loaded from the JSON file generated by
    :func:`search_stac_items`. Successfully acquired items are tracked in a
    ``completed_items.json`` file, allowing interrupted tasks to resume
    without re-acquiring completed items.

    Parameters
    ----------
    unit : dict
        Sensor-label work unit containing the STAC item metadata path and
        sensor acquisition parameters.

    Returns
    -------
    dict
        The updated work unit containing paths to the acquired Zarr datasets.

    Raises
    ------
    Exception
        Re-raises an acquisition error after logging the failed item.
        Airflow retries the task according to its retry configuration.

    Notes
    -----
    Acquired datasets are stored under::

        <save_dir>/interim/images/<sensor>/<sensor>_<label_name>/
    """
    y_name, _, sensor, sensor_params, _ = _get_unit_info(unit)
    items_path = Path(sensor_params["items_path"])
    items = [pystac.Item.from_dict(item) for item in json.loads(items_path.read_text())]
    completed_items_path = Path(f"{sensor_params['save_dir']}/interim/images/{sensor}/{sensor}_{y_name}/completed_items.json")
    completed_items_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"filename: {y_name}")
    
    if completed_items_path.exists():
        completed_items_list = json.loads(completed_items_path.read_text())
        logger.info(f"completed list: {completed_items_list}")
        items = [item for item in items if item.id not in completed_items_list]
    else:
        completed_items_list = []
    logger.info(f"number of items need to download: {len(items)}")
    for item in items:
        basename = f"interim/images/{sensor}/{sensor}_{y_name}/{item.id}"
        try:
            logger.info(f"sensor params: {sensor_params}")
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
def align_items(unit):
    """
    Align sensor items to a common spatial grid.
    
    The first Zarr item is used as the spatial reference. Subsequent
    items are reprojected to match its CRS, resolution, extent, and
    coordinates. Successfully aligned items are saved as Zarr files.

    Parameters
    ----------
    unit : dict
        Processing unit containing the target information, sensor
        parameters, input Zarr paths, bounding box, and chunk settings.

    Returns
    -------
    dict
        The input unit updated with paths to the aligned Zarr items.
    """
    y_name, _, sensor, sensor_params, _ = _get_unit_info(unit)
    zarr_paths = sensor_params["zarr_paths"]
    proj_epsg =  bbox_to_epsg(*sensor_params["bbox"])
    logger.info(f"filename: {y_name}, num of zarr: {len(zarr_paths)}")

    id_ref = os.path.basename(zarr_paths[0])
    basename_ref = f"interim/images/{sensor}_aligned/{sensor}_{y_name}/{id_ref}"
    ds_ref = xr.open_zarr(zarr_paths[0], consolidated=False)
    ds_ref = ds_ref.rio.write_crs(f"epsg:{proj_epsg}", inplace=True)
    sensor_params["encoding"] = {var: {"chunks": (sensor_params["chunks"]["x"], sensor_params["chunks"]["y"])} 
                                                        for var in ds_ref.data_vars 
                                                        if "x" in ds_ref[var].dims and "y" in ds_ref[var].dims}
    aligned_paths = []
    non_aligned_paths = []
    aligned_path = save_xarray(ds_ref, filename=basename_ref, save_format='zarr', **sensor_params)
    aligned_paths.append(aligned_path)
    for i, zarr_path in enumerate(zarr_paths[1:]):
        id = os.path.basename(zarr_path).split(".")[0]
        basename  = f"interim/images/{sensor}_aligned/{sensor}_{y_name}/{id}"
        logger.info(f"{i+1} checking {id} to {id_ref}")
        ds = xr.open_zarr(zarr_path, consolidated=False)
        ds = ds.rio.write_crs(f"epsg:{proj_epsg}", inplace=True)
        ds = ds.rio.reproject_match(ds_ref)
        check_x_coord = ds.x.equals(ds_ref.x)
        check_y_coord = ds.y.equals(ds_ref.y)
        if check_x_coord and check_y_coord: 
            aligned_path = save_xarray(ds, filename=basename, save_format='zarr', **sensor_params)
            aligned_paths.append(aligned_path)
        else: 
            non_aligned_paths.append(zarr_path)
            logger.warning(f"x and y coord of {id} are not aligned with ref {id_ref}")
    
    sensor_params["aligned_paths"] = aligned_paths
    sensor_params["non_aligned_paths"] = non_aligned_paths
    return unit

@task
def get_median(unit:dict):
    """
    Create a temporal median composite from aligned sensor items.
    
    If more than ``max_item`` are available, items are filtered using
    the item IDs from the associated item metadata file. The remaining
    items are concatenated along the time dimension and reduced using
    a median composite.

    Parameters
    ----------
    unit : dict
        Processing unit containing sensor parameters and aligned Zarr
        paths.
    max_items : int, default=52
        Maximum number of items before metadata-based filtering is
        applied to control memory usage.

    Returns
    -------
    dict
        The input unit updated with the path to the median composite.
    """
    y_name, _, sensor, sensor_params, task_params = _get_unit_info(unit)
    zarr_paths = sensor_params["aligned_paths"]
    max_items = task_params["median_max_item"]
    logger.info(f"filename: {y_name}, available num of zarr: {len(zarr_paths)}")

    if len(zarr_paths) > max_items:
        logger.info(f"filter the zarrs since the available zarrs > {max_items}")
        items_list = json.loads(Path(sensor_params["items_path"]).read_text())
        item_ids = [item_dict["id"] for item_dict in items_list]
        zarr_ids = [zarr_path.split("/")[-1].split(".")[0] for zarr_path in zarr_paths]
        filtered_paths = []
        for i, zarr_id in enumerate(zarr_ids):
            filtered_paths.append(zarr_paths[i]) if zarr_id in item_ids else None
        zarr_paths = filtered_paths
        logger.info(f"number of searched items: {len(items_list)}")
        logger.info(f"post filter number of zarr: {len(zarr_paths)}")

    _review_temporal_availability(zarr_paths, sensor)

    basename  = f"interim/images/{sensor}_median/{sensor}_{y_name}"
    data_vars = xr.open_zarr(sensor_params["zarr_paths"][0]).data_vars
    logger.info(f"zarr_paths: {len(zarr_paths)}, data vars: {data_vars}, ")
    ds = xr.open_mfdataset(zarr_paths, consolidated=True, engine="zarr", 
                           combine="nested", concat_dim="time", coords="minimal", parallel=False, chunks=sensor_params.get("chunks", "auto"), 
                           data_vars='all')
    if sensor == "S2":
        logger.info(f"sensor {sensor}: delete scl")
        ds = ds.drop_vars(["SCL", ])
    ds = calculate_median_composite(ds, dim="time")
    ds = ds.chunk({"x":sensor_params["chunks"]["x"], "y":sensor_params["chunks"]["y"]})
    logger.info(f"data vars: {ds.data_vars}")
    sensor_params["encoding"] = {var: {"chunks": (sensor_params["chunks"]["x"], sensor_params["chunks"]["y"])} 
                                                 for var in ds.data_vars 
                                                 if "x" in ds[var].dims and "y" in ds[var].dims}
    zarr_path = save_xarray(ds, filename=basename, save_format='zarr', **sensor_params)
    sensor_params["median_path"] = zarr_path
    return unit

@task
def construct_sensor_label_dataset(unit: dict):
    """
    Construct the final sensor-label (X-Y) dataset.

    The preprocessed sensor dataset is loaded, assigned its CRS, rechunked,
    and combined with the corresponding label GeoTIFF to create the final
    paired dataset.

    Parameters
    ----------
    unit : dict
        Sensor-label work unit containing the preprocessed sensor dataset,
        label path, and sensor-specific parameters.

    Returns
    -------
    dict
        The updated work unit containing the path to the final sensor-label
        paired dataset.

    Notes
    -----
    The resulting dataset is stored under::

        <save_dir>/final/images/<sensor>_xy_pair/

    The ``time`` chunk configuration is removed before constructing the
    final X-Y dataset because the output is spatially paired with the label.
    """
    y_name, y_path, sensor, sensor_params, _ = _get_unit_info(unit)
    region, zone = y_name.split("_")[:2]
    if sensor == "S2":
        sensor_name = "sentinel2"
    elif sensor == "S1":
        sensor_name = "sentinel1"
    else:
        sensor_name = sensor
    logger.info(f"check name parse {region},{zone} ")
    basename  = f"final/images/{sensor_name}_{region}_reduce2020_labelclass1_gapfilled/{sensor_name}_{region}_{zone}_reduce2020_labelclass1_gapfilled"
    proj_epsg =  bbox_to_epsg(*sensor_params["bbox"])
    sensor_params["chunks"].pop("time")
    x_ds = xr.open_zarr(sensor_params["median_path"], consolidated=True)
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
    """
    Build sensor-label datasets from label GeoTIFFs and satellite data.

    The DAG creates one processing work unit for every combination of a
    label GeoTIFF and configured sensor. Each work unit is processed through
    the :func:`process_aoi` task group.

    The workflow consists of:

    1. Discovering label GeoTIFF files.
    2. Creating label-sensor work units.
    3. Searching and sampling STAC items.
    4. Acquiring sensor data.
    5. Concatenating acquired datasets.
    6. Applying sensor-specific preprocessing.
    7. Constructing sensor-label paired datasets.

    DAG Parameters
    --------------
    y_dir : str
        Directory containing label GeoTIFF files.

    sensors : dict
        Sensor configuration dictionary. Each sensor entry contains the
        parameters required for STAC search, data acquisition, chunking,
        preprocessing, and output generation.

    Notes
    -----
    The DAG is manually triggered because ``schedule=None`` and does not
    perform historical backfilling because ``catchup=False``.
    """
    unit = search_stac_items(unit)
    unit = acquire_stac_items(unit)
    unit = align_items(unit) 
    unit = get_median(unit)
    unit = construct_sensor_label_dataset(unit)

@dag(
    dag_id="sensor_label_pair_AI4LCC_CNRS",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "y_dir": Param("../data/labels", type="string"),
        "sensors":Param({
                        "S2":{
                        "datetime": "2020-01-01/2021-01-01",
                        "bands": [
                                    "SCL", "B01", "B02", "B03",
                                    "B04", "B05", "B06", "B07",     
                                    "B08", "B8A", "B09", "B11", 
                                    "B12",
                                ] ,
                        "save_dir": "../data/",
                        "query": {"eo:cloud_cover": {"lte": 20}},
                        "max_items": 1000,
                        "chunks": {"x": 256, "y": 256, "time": 1},
                        "resolution":10,
                        "is_save_tif": True,
                        "dtype": "uint16",
                        "nodata": 0,
                        "consolidated": True,
                        "zarr_format":3, 
                        "save_preprocessed": True}
                        }
                    ,type="object", items={"type":"object"}),
        "tasks": Param({
            "median_max_item":52,
            "item_interval_day":7,
            }, type="object", )
        
    }
)
def xy_builder():
    """
    Build sensor-label datasets using a dynamically mapped workflow.

    The DAG discovers label GeoTIFF files, creates one work unit for each
    label-sensor combination, and processes each work unit through the
    ``process_aoi`` task group.

    The resulting workflow performs STAC search, sensor-data acquisition,
    temporal concatenation, preprocessing, and sensor-label dataset
    construction.

    DAG Parameters
    --------------
    y_dir : str
        Directory containing the label GeoTIFF files.

    sensors : dict
        Dictionary containing sensor names and their processing
        configurations. Each sensor configuration defines parameters such
        as the temporal search range, bands, STAC query, output directory,
        spatial resolution, chunk sizes, and preprocessing options.

    Notes
    -----
    The DAG uses dynamic task mapping to process each label-sensor
    combination independently.

    The number of work units is approximately:

    ``number of label files × number of configured sensors``
    """
    work_units = build_work_unit(get_y_files())
    process_aoi.expand(unit=work_units)

xy_builder()