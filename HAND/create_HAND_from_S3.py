import argparse
import logging
import os
import sys
from pathlib import Path

import boto3
import matplotlib.pyplot as plt
import numpy as np
from osgeo import gdal
from tqdm import tqdm
from asf_tools.water_map import make_water_map

S3_CLIENT = boto3.client('s3')
bucket = 'hyp3-nasa-disasters'
prefix = 'Honduras'

scene_names = []
test = 0

#log.info('Downloading scenes')

local_data = Path(__file__).parent / "HAND/data"
for key in tqdm(S3_CLIENT.list_objects(Bucket=bucket, Prefix=prefix)['Contents']):
    if key["Key"].endswith("_vv.tif"):
        keyname = key["Key"]
        scene = keyname.replace('Honduras/', '')
        S3_CLIENT.download_file(bucket, keyname, str(Path(local_data / scene)))
        S3_CLIENT.download_file(bucket, keyname.replace('_vv.tif', '_vh.tif'),
                                str(Path(local_data / scene.replace('_vv.tif', '_vh.tif'))))
        scene_names.append(scene.replace('_vv.tif', ''))

#log.info('Creating water_maps')
#for scene in tqdm(scene_names):
 #   VV = Path(local_data / scene + '_vv.tif')
 #   VH = Path(local_data / scene + '_vh.tif')
 #   water_map = make_water_map(scene + '_WM.tif', VV, VH)
