# -*- coding: utf-8 -*-
"""
Created on Tue Jan 27 15:18:00 2015

@author: bwoods
"""
from datetime import datetime
import os
import time
import logging
import subprocess

import xml.etree.ElementTree as etree
from xml.etree.ElementTree import ParseError
import numpy as np
GDALLOCATIONINFO='/home/twu/miniconda2/envs/pir_testing/bin/gdallocationinfo'
from osgeo import gdal

logger = logging.getLogger("PIR")


def get_value_gdalinfo(raster_file, mx, my, band=1):
    '''
    Get an XML file with the pixel information
    '''
    # this can (possibly) be sped up using the -b band and -valonly options
    cmd = ['gdallocationinfo', '-xml', '-wgs84', raster_file, str(mx), str(my)]
    TIME_THRESHOLD = 3.0

    proc_stdout = ""
    value = None
    time0 = time.time()
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        proc_stdout = proc.stdout.read()
        root = etree.fromstring(proc_stdout)
        txt = root[band-1][0].text
        value = float(txt) if '.' in txt else int(txt)
    except (OSError, ParseError), e:
        logger.error("(%s) problem accessing %s %s %s", type(e).__name__, raster_file, mx, my)
        logger.error("     %s", cmd)
        logger.exception(e)
    except BaseException, e:
        logger.error("     %s", cmd)
        logger.error("(BaseException) could not parse output from querying %s %s %s", raster_file, mx, my)
        raise

    get_time = time.time() - time0
    if get_time > TIME_THRESHOLD:
        logger.warn("time to get point_data exceeds threshold: %s > %s (%s)",
                    get_time, TIME_THRESHOLD, raster_file)
    else:
        logger.debug("time to get point_data: %s <= %s (%s)",
                    get_time, TIME_THRESHOLD, raster_file)
    return value


def get_value_at_point(raster_file, mx, my, band, center_coord=True,
                       use_XML=True):
    '''
    Get the value at point (mx, my) of *band* in GDAL rasterfile *fRaster*
    If *center_coord* then we are using center coordinates so shift to corner

    If neighboring_points == True, return a 3x3 grid processed with the
    aggregator provided by neighbor_op
    '''

    if use_XML is True:
        return get_value_gdalinfo(raster_file, mx, my, band)
    else:
        logger.debug('Extract the value by loading the whole grid')
        gdata = gdal.Open(raster_file)
        (ul_x, dx, x_rot, ul_y, y_rot, dy) = gdata.GetGeoTransform()

        rb = gdata.GetRasterBand(band)
        data = rb.ReadAsArray().astype(np.float)
        gdata = None  # this is how you close a GDAL dataset in python

        # handle center vs corner coordinates
        if center_coord:
            xc, yc = mx - dx / 2.0, my - dy / 2.0
        else:
            xc, yc = mx, my

        x = np.round((xc - ul_x)/dx).astype(int)
        y = np.round((yc - ul_y)/dy).astype(int)
        # get the local point value
        point_value = data[y, x]
        return point_value


def load_DEM_at_point(x, y, input_dir):
    '''
    Load the DEM values at a point *x, y*.
    Searches *input_dir* to find the DEM tiles

    Return the elevation, slope_code
    '''
    # find the file to use
    f = find_DEM_file(x, y, input_dir)
    logger.debug('Matched DEM file %s', f)

    # then load the value for the DEM and slope
    elev = get_value_at_point(f, x, y, 5)
    slope_code = get_value_at_point(f, x, y, 4)
    logger.debug('Elevation %f, slope %i', elev, slope_code)

    return elev, slope_code


def summarize_bounds(raster_file):
    '''
    Find the bounds of the file
    '''
    gdata = gdal.Open(raster_file)
    nx, ny = gdata.RasterXSize, gdata.RasterYSize,
    (ul_x, dx, x_rot, ul_y, y_rot, dy) = gdata.GetGeoTransform()
    ll_x = ul_x
    ll_y = ul_y + dy * ny
    ur_x = ul_x + dx * nx
    ur_y = ul_y
    lr_x = ul_x + dx * nx
    lr_y = ul_y + dy * ny
    logger.info('Grid spacing dx %s, dy %s', dx, dy)
    logger.info("Upper Left (%f, %f)", ul_x, ul_y)
    logger.info("Lower Left (%f, %f)", ll_x, ll_y)
    logger.info("Upper Right (%f, %f)", ur_x, ur_y)
    logger.info("Lower Right (%f, %f)", lr_x, lr_y)


def find_DEM_file(lon, lat, tile_dir):
    '''
    Locate the appropriate DEM tile to use for point *lat*, *lon*
    This assumes the point is a center coordinate so to
    '''
    xi, yi = np.ceil(-lon), np.ceil(lat)
    fPath = os.path.join(tile_dir, 'n%02iw%03i_pix.pix' % (yi, xi))
    return fPath


if __name__ == '__main__':
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser(description="Find the coordinates of a pint",
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('longitude', type=float,
                        help="Longitude of the point")
    parser.add_argument('latitude', type=float,
                        help="Latitude of the point")
    parser.add_argument('--hazfile', type=str,
                        help='Example hazard file',
                        default='/Users/bwoods/temp/final_lastdate.tif')
    parser.add_argument("--log", dest='logfile', type=str,
                        help="Log file. If not given, then log messages to"
                             " the screen using stderr.",
                        default=None)
    parser.add_argument("--verbose", "-v", action='count',
                        help='Increase logging verbosity (repeat for more'
                             ' verbose logs)', default=0)

    args = parser.parse_args()

    loglevs = {0: logging.ERROR,
               1: logging.WARNING,
               2: logging.INFO,
               3: logging.DEBUG}
    if args.verbose > 3:
        args.verbose = 3

    log_level = loglevs[args.verbose]

    if args.logfile is not None:
        logging.basicConfig(filename=args.logfile, filemode='a',
                            format="%(message)s", level=log_level)
    else:
        logging.basicConfig(level=log_level)

    # DEM loading
    tic = datetime.now()
    values = load_DEM_at_point(args.longitude, args.latitude)
    logger.debug("Elevation %f Slope %s", **values)
    logger.debug("DEM time ellapsed %s", datetime.now() - tic)

    # test single hazard loading
    toc = datetime.now()
    values = get_value_at_point(args.hazfile, args.longitude, args.latitude,
                                band=1, use_XML=False)
    logger.debug("single hazard value %s", values)
    logger.debug("hazard time ellapsed %s, total %s",
                 datetime.now()-toc, datetime.now() - tic)

    # test gdal command line load
    toc = datetime.now()
    values = get_value_gdalinfo(args.hazfile, args.longitude, args.latitude)
    logger.debug("gdal returned value %s", values)
    logger.debug("gdal time ellapsed %s, total %s",
                 datetime.now()-toc, datetime.now() - tic)
