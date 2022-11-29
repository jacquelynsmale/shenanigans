from pathlib import Path
import logging
import argparse
import os
import sys

from tqdm import tqdm
import tempfile
import boto3
import dask
import dask.bag as db
from dask.diagnostics import ProgressBar
from asf_tools.water_map import make_water_map

log = logging.getLogger(__name__)

S3_CLIENT = boto3.client('s3')
local_data = '/User/jrsmale/GitHub/shenanigans/HAND/data/'
bucket = 'hyp3-nasa-disasters'
prefix = 'Honduras'


def prep_new_water_map(scene):
        log.info(f'Prepping {scene}')
        try:
            vv_path = str(Path(local_data + scene + '_VV.tif'))
            vh_path = str(Path(local_data + scene + '_VH.tif'))
            wm_path = str(Path(local_data + scene + '_WM.tif'))
            make_water_map(wm_path, vv_path, vh_path)
        except:
            with tempfile.TemporaryDirectory() as tmpdir:
                vv_path = str(Path(tmpdir + scene + '_VV.tif'))
                vh_path = str(Path(tmpdir + scene + '_VH.tif'))
                wm_path = str(Path(local_data + scene + '_WM.tif'))

                keyname = prefix + '/' + scene + '_VV.tif'
                S3_CLIENT.download_file(bucket, keyname, vv_path)
                S3_CLIENT.download_file(bucket, keyname.replace('_VV.tif', '_VH.tif'), vh_path)
                make_water_map(wm_path, vv_path, vh_path)

        S3_CLIENT.upload_file(wm_path, bucket, 'Honduras/New_WM/' + scene + '_WM.tif')
        log.info(f'Uploaded {scene}')
        file_path = str(Path(local_data + scene))

        return


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('scene_file', help='VSICURL path to a Copernicus GLO-30 DEM GeoTIFF to calculate HAND from. '
                                         'Tile must be located in the AWS Open Data bucket `s3://copernicus-dem-30m/`. '
                                         'For more info, see: https://registry.opendata.aws/copernicus-dem/')
    parser.add_argument('-v', '--verbose', action='store_true', help='Turn on verbose logging')
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=level)
    log.debug(' '.join(sys.argv))
    log.info(f'Making HAND tile from {args.scene_file}')

    with open(args.scene_file, 'r') as f:
        scenes = f.read()
    scenes=scenes.split(', ')

    for scene in tqdm(scenes):
        prep_new_water_map(scene)


if __name__ == '__main__':
    main()
