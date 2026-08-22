import logging

import rasterio as rio
import utm
from pyproj import Transformer
from pyproj.aoi import AreaOfInterest
from pyproj.database import query_utm_crs_info

logger = logging.getLogger(__name__)

def get_bbox(lon, lat, half_size_m):
    easting, northing, zone_number, zone_letter = utm.from_latlon(lat,lon)
    xmin = easting - half_size_m
    xmax = easting + half_size_m
    ymin = northing - half_size_m
    ymax = northing + half_size_m
    min_lat, min_lon = utm.to_latlon(xmin, ymin, zone_number, zone_letter) 
    max_lat, max_lon = utm.to_latlon(xmax, ymax, zone_number, zone_letter)
    return [min_lon.item(), min_lat.item(), max_lon.item(), max_lat.item()]

def bbox_to_epsg(min_lon, min_lat, max_lon, max_lat):
    crs_list = query_utm_crs_info(
        datum_name="WGS 84",
        area_of_interest=AreaOfInterest(
            west_lon_degree=min_lon,
            south_lat_degree=min_lat,
            east_lon_degree=max_lon,
            north_lat_degree=max_lat,
        ),
    )
    return crs_list[0].code if crs_list else None

def get_bbox_from_tif(path):
    src = rio.open(path)
    bbox = src.bounds
    crs = src.crs
    transformer = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True)
    min_lon, min_lat = transformer.transform(bbox.left, bbox.bottom)
    max_lon, max_lat = transformer.transform(bbox.right, bbox.top)
    return [min_lon, min_lat, max_lon, max_lat]

