import xarray

def cloud_masking_s2(ds:xarray.Dataset, mask_class:list, drop_scl=True):
    mask = ds['SCL'].isin(mask_class)
    ds = ds.where(~mask)
    if drop_scl:
        ds = ds.drop_vars("SCL")
    return ds

def median_composite(ds:xarray.Dataset, dim='time'):
    ds = ds.median(dim=dim, skipna=True)
    return ds

def reflectance_scalling_s2(ds:xarray.Dataset):
    return ds/10_000