import os
import subprocess
import sys
import logging
import uuid
from urllib.parse import unquote_plus

# set up logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

# imports for actual work
os.environ['PROJ_LIB'] = '/python/pyproj/proj_dir/share/proj'

import rasterio
import rasterio.mask
import boto3
import numpy as np
from pyproj import Proj, transform
import fiona
from fiona.crs import from_epsg

# imports to unzip shp:
import json
import zipfile

s3_client = boto3.client('s3')

# reproject the shape file
def lambda_handler(event, context):
    """ Lambda handler """
    logger.debug(event) # if logging needed
    # logger.info()
    
    # register the event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        
        dir_id = uuid.uuid4()
        download_path = '/tmp/{}{}'.format(dir_id, tmpkey)
        shp_path = '/tmp/{}/'.format(dir_id)
        upload_path = '/tmp/{}/Landsat8-{}.tif'.format(dir_id, tmpkey.replace('.zip', ''))
        
        s3_client.download_file(bucket, key, download_path)
        
        # unzip the file
        os.mkdir(shp_path)
        with zipfile.ZipFile(download_path, "r") as zip_ref:
            print(zip_ref)
            zip_ref.extractall(shp_path)
            
        # file_loc = name of .shp file in the unzipped directory, take the first one
        file_loc = [f for f in os.listdir(shp_path) if f.endswith('.shp')][0]
        logger.debug([f for f in os.listdir(shp_path) if f.endswith('.shp')]) # if logging needed
        
        # load shape file
        shape = fiona.open(os.path.join(shp_path, file_loc))
        original = Proj(shape.crs)
        destination = Proj('EPSG:3577')
        
        # reproject to EPSG:3577
        for feat in shape: # feat = one polygon of the shapefile
            out_linearRing = [] # empty list for the LinearRing of transformed coordinates
            for point in feat['geometry']['coordinates'][0]: # LinearRing of the Polygon
                long,lat =  point  # one point of the LinearRing
                x,y = transform(original, destination,long,lat) # transform the point
                out_linearRing.append((x,y)) # add all the points to the new LinearRing
            # transform the resulting LinearRing to  a Polygon and write it
            feat['geometry']['coordinates'] = [out_linearRing]
        shapes = [feat['geometry']]

        # clip raster
        rasterio.Env(CPL_CURL_VERBOSE=True)
        src = rasterio.open(f's3://solve-landsat8/landsat8_30y_merge_co.tif')
        out_image, out_transform = rasterio.mask.mask(src, shapes, crop=True)
        out_meta = src.meta
        print("d")
        
        # save raster out
        out_meta.update({"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})
        dest = rasterio.open(upload_path, "w", **out_meta)
        dest.write(out_image)
        dest.close()
        print("e")
        
        size = os.path.getsize(upload_path)
        print(upload_path)
        print('Size of file is', size, 'bytes')
        
        # put to S3
        s3_client.upload_file(upload_path, 'solve-landsat8-input', 'Landsat8-{}.tif'.format(tmpkey.replace('.zip', '')))
        print("f")
        
        return({
            'status_code': 200,
            'body': json.dumps('file is created in:'+upload_path)
        })
