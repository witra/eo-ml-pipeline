import gc
import json
import logging
import math

import dask
import xarray as xr
from tqdm import tqdm

logger = logging.getLogger(__name__)

def calculate_mean_std(paths, vars, prev_compute:dict | None=None, outpath='./mean_std.json', **kwargs):
    """Calculate global mean and standard deviation for Zarr variables.

    Computes global statistics across multiple Zarr datasets while ignoring
    NaN values. Previously computed aggregate statistics can be provided to
    continue or update an existing computation.

    Parameters
    ----------
    paths : iterable of str
        Paths to the Zarr datasets.
    vars : iterable of str
        Variable names for which statistics are calculated.
    prev_compute : dict, optional
        Previously computed aggregate statistics containing ``total_sum``,
        ``total_sq``, and ``total_n`` for each variable.
    outpath : str, default="./mean_std.json"
        Path where the computed statistics are saved as a JSON file.
    **kwargs
        Additional parameters reserved for future use.

    Returns
    -------
    str
        Path to the output JSON file.
    """
    stats = {}
    for var in vars:
        if prev_compute:
            total_sum = prev_compute[var]['total_sum']
            total_sq = prev_compute[var]['total_sq']
            total_n = prev_compute[var]['total_n']
        else: 
            total_sum = 0
            total_sq = 0
            total_n = 0

        for path in tqdm(paths):
            ds = xr.open_zarr(path, consolidated=False, drop_variables=['label', 'spatial_ref'], chunks="auto")
            ds_var = ds[var]
            ds_var_sq = ds_var**2

            sum_task = ds_var.sum(skipna=True)
            sq_task = ds_var_sq.sum(skipna=True)
            n_task = ds_var.count()
            sum_result, sq_result, n_result = dask.compute(sum_task, sq_task, n_task)

            total_sum += sum_result.item()
            total_sq += sq_result.item()
            total_n += n_result.item()
            del ds
            gc.collect()
        mean = total_sum/total_n
        std = math.sqrt((total_sq/total_n) - mean**2)
        stats[var] = {
            "mean": mean,
            "std": std,
            "total_sum": total_sum,
            "total_sq": total_sq,
            "total_n": total_n
        }
    with open(outpath, "w") as f:
        json.dump(stats, f, indent=4)
    return outpath