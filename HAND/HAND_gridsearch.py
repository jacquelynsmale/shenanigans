import os
import argparse
import matplotlib.pyplot as plt
from osgeo import gdal
import logging
import sys

import numpy as np

from asf_tools.hand.cal_hand_by_dem import make_asf_hand

log = logging.getLogger(__name__)


def plot_hand(dem_tile: str, raster_dir, acc_range=np.linspace(200, 1600, 29)):
    if raster_dir is None:
        raster_dir = os.getcwd()

    num_zeros = np.zeros(len(acc_range))
    num_nan = np.zeros(len(acc_range))

    dem_name = dem_tile.split('/')
    hand_base = dem_name[-1].replace('DEM.tif', 'HAND_')

    for i, acc_thresh in enumerate(acc_range):
        print(acc_thresh)
        try:
            hand_raster = raster_dir + '/' + hand_base + str(acc_thresh) + '.tif'
            hand_array = gdal.Open(str(hand_raster), gdal.GA_ReadOnly).ReadAsArray()
        except:
            print(f'raster for acc={acc_thresh} doesnt exist! Creating')
            hand_raster, acc_raster = make_asf_hand(dem_tile, acc_thresh=acc_thresh)
            hand_array = gdal.Open(str(hand_raster), gdal.GA_ReadOnly).ReadAsArray()
        zeros = np.where(hand_array == 0)
        num_zeros[i] = len(zeros[0])
        nans = np.isnan(hand_array)
        nans_tf = np.where(nans == True)
        num_nan[i] = len(nans_tf[0])

    return acc_range, num_zeros, num_nan


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('dem_tile', help='VSICURL path to a Copernicus GLO-30 DEM GeoTIFF to calculate HAND from. '
                                         'Tile must be located in the AWS Open Data bucket `s3://copernicus-dem-30m/`. '
                                         'For more info, see: https://registry.opendata.aws/copernicus-dem/')
    parser.add_argument('-d', '--raster_dir', help='Path to local directory where HAND files are stored')
    parser.add_argument('-v', '--verbose', action='store_true', help='Turn on verbose logging')
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=level)
    log.debug(' '.join(sys.argv))
    log.info(f'Making HAND tile from {args.dem_tile}')

    acc_range, num_zeros, num_nan = plot_hand(args.dem_tile, args.raster_dir)

    dem_name = args.dem_tile.split('/')

    plt.scatter(acc_range, num_zeros)
    plt.title(f'Number of Zeros for {dem_name[8]}')
    plt.ylabel('Number of zeros')
    plt.xlabel('ACC thresh')
    plt.show()

    plt.scatter(acc_range, num_nan)
    plt.title(f'Number of NAN for {dem_name[8]}')
    plt.ylabel('Number of NAN')
    plt.xlabel('ACC thresh')
    plt.show()


if __name__ == '__main__':
    main()
