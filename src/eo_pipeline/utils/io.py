import logging

logger = logging.getLogger(__name__)

def save_xarray(ds, save_dir, filename, save_format='zarr', **kwargs):
    if save_format == 'zarr':
        path = f'{save_dir}/{filename}.zarr'
        encoding = kwargs.get("encoding", None)
        ds.to_zarr(path, mode="w", consolidated=False, zarr_format=3, encoding=encoding)
        return path
    elif save_format == 'tif':
        path = f'{save_dir}/{filename}.tif'
        ds.rio.to_raster(path, driver="COG", compress="ZSTD", blocksize=512, overview_resampling="nearest")
        return path
    else:
        logger.info(f'the save_format {save_format} is not available yet')