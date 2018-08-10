# -*- coding: utf-8 -*-
"""
Created on Tue Feb 10 14:31:24 2015

@author: bwoods
"""

from datetime import datetime
import logging
import os

from PIR.file_handler import load_DEM_at_point, get_value_at_point

import PIR.output_xml as oxml


logger = logging.getLogger("PIR")

__BASE_YEAR = 2010
__MAX_TRY_YEARS = 3

def get_peril(peril_name, currency_date, srcDir, longitude, latitude, n_years=None):
    '''
    Load the record information for *peril_name* from *srcDir* and *peril_name*.
    Args:
        peril_name (str): e.g., Lightning, Hail, Fire, Wind
        currency_date (DateTime)
        srcDir (str or os.path)
        longitude (float)
        latitude (float)
        n_years (int): optional for defining how far back in history to query

    Returns:
        output_xml.Peril
    '''
    if n_years is None:
        n_years = currency_date.year - __BASE_YEAR

    # files will be dated the month _after_ their currency date
    # e.g., currency date of 2014-12-31 will be in files dated Jan. 2015
    peril_letter = peril_name[0].upper()
    file_prefix = currency_date.strftime('%m%Y')
    file_dir = os.path.join(srcDir, peril_name)

    # YTD files have an annoying unique pattern
    fYTD_1km = os.path.join(file_dir, '%s_%s_YTD1km.tif' % (file_prefix,
                                                            peril_letter))
    at_point_ytd = get_value_at_point(fYTD_1km, longitude, latitude, band=1)
    fYTD_3km = os.path.join(file_dir, '%s_%s_YTD3km.tif' % (file_prefix,
                                                            peril_letter))
    neighbor_ytd = get_value_at_point(fYTD_3km, longitude, latitude, band=1)

    # initialize list of peril objects for each year
    yearly_list = [oxml.Annual_Peril(1, at_point=at_point_ytd,
                                     neighboring=neighbor_ytd)]

    # loop through the remaining years (after current year)
    for year in xrange(2, n_years+1):
        f1km = os.path.join(file_dir, '%s_%s_%iyr1km.tif' % (file_prefix,
                                                             peril_letter,
                                                             year))
        at_point = get_value_at_point(f1km, longitude, latitude, band=1)
        f3km = os.path.join(file_dir, '%s_%s_%iyr3km.tif' % (file_prefix,
                                                             peril_letter,
                                                             year))
        neighbor = get_value_at_point(f3km, longitude, latitude, band=1)
        yearly_list.append(oxml.Annual_Peril(year, at_point=at_point,
                                             neighboring=neighbor))

    # 5 YTD median and anomaly
    # so we are building the years history from 2010 and keep growing...

    first_year = currency_date.year - __BASE_YEAR

    fMed = None
    ytd_year = None
    for offset in range(2):
        year = first_year - offset
        logging.getLogger(__name__).debug("offset: %s, year: %s", offset, year)
        tryfMed = os.path.join(file_dir, '%s_%s_M%dYR1km.tif' % (file_prefix, peril_letter, year))
        if os.path.exists(tryfMed):
            fMed = tryfMed
            ytd_year = year
            break

    if fMed is None:
        raise PIR_Data_Error("Can't find median and anomaly file for %s in %s/%s",
                             file_dir, peril_name, file_prefix)

    else:
        median = get_value_at_point(fMed, longitude, latitude, band=1)
        # no median means there can be no anomaly
        if median == 0 and at_point_ytd == 0:
            anomaly = 0
        elif median == 0 and at_point_ytd != 0:
            anomaly = 100
        else:
            fAnom = os.path.join(file_dir, '%s_%s_Ch%dYR1km.tif' % (file_prefix, peril_letter, ytd_year))
            anomaly = get_value_at_point(fAnom, longitude, latitude, band=1)

    # get the date of the last event at the 1km level
    fLast = os.path.join(file_dir, '%s_%s_LastDate.tif' % (file_prefix,
                                                           peril_letter))
    at_point = get_value_at_point(fLast, longitude, latitude, band=1)
    # reformat the date
    if at_point == 0:
        event_date = 'NULL'
    else:
        event_date = datetime.strptime(str(at_point), '%Y%m%d').strftime('%Y-%m-%d')

    return oxml.Peril(yearly_list, median, anomaly,
                      oxml.Last_Event(event_date), currency_date)


def get_hail_risk(currency_date, srcDir, longitude, latitude, n_years=None):
    '''
    Get the hail damage and risk scorse from *currency_date* from
    directory *srcDir* at a poin *longitude, latitude*
    '''

    if n_years is None:
        n_years = currency_date.year - __BASE_YEAR

    file_prefix = currency_date.strftime('%m%Y')
    file_dir = os.path.join(srcDir, 'HailRisk')

    # single Risk Score
    f = os.path.join(file_dir, '%s_HailRisk_RiskScore.tif' % file_prefix)
    risk_score = get_value_at_point(f, longitude, latitude, band=1)

    # get the Hail Damage Scores for the last 5 years
    hail_list = []
    for yr in range(1, n_years+1):
        fCt = os.path.join(file_dir, '%s_HailRisk_Ct%iyr.tif' % (file_prefix,
                                                                 yr))
        Ct = get_value_at_point(fCt, longitude, latitude, band=1)
        fSc = os.path.join(file_dir, '%s_HailRisk_DSc%iyr.tif' % (file_prefix,
                                                                  yr))
        DSc = get_value_at_point(fSc, longitude, latitude, band=1)

        hail_list.append(oxml.Hail_Damage_Score(yr, score=DSc, events=Ct))

    hail_scores = oxml.Hail_Damage(hail_list, currency_date)

    return risk_score, hail_scores

def get_wind_risk(currency_date, srcDir, longitude, latitude):
    '''
    Get the hail damage and risk scorse from *currency_date* from
    directory *srcDir* at a poin *longitude, latitude*
    '''
    file_prefix = currency_date.strftime('%m%Y')
    file_dir = os.path.join(srcDir, 'Wind')

    # single Risk Score
    f = os.path.join(file_dir, '%s_Wind_RiskScore.tif' % file_prefix)
    risk_score = get_value_at_point(f, longitude, latitude, band=1)

    return risk_score

def process_point(location, transaction, cfg, peril_list):
    '''
    Processes the values for each hazard specified in *hazards* at the
    specified longitude and latitude

    *cfg* is the config file dictionary
    '''
    peril_list = [peril.upper() for peril in peril_list]

    # unpack needed value from config file dictionary
    srcDir = cfg['data_dir']
    demDir = os.path.join(srcDir, 'Slope')
    errors = []

    try:
        risk_score = None
        hail_scores = None
        wind_risk_score = None
        if 'HAIL' in peril_list:
            hail_risk_score, hail_scores = get_hail_risk(cfg['Hail_Damage_Score']['currency'],
                                                         srcDir,
                                                         location.lon, location.lat)

        if 'WIND' in peril_list:
            wind_risk_score = get_wind_risk(cfg['Wind_Damage_Score']['currency'],
                                            srcDir,
                                            location.lon, location.lat)

        # DEM value
        elevation = None
        slope = None
        if 'TERRAIN' in peril_list:
            elevation, slope = load_DEM_at_point(location.lon, location.lat, demDir)


    except PIR_Data_Error as e:
        logger.exception('could not process_point: %s', e)
        raise
    except BaseException as e:
        logger.exception('Encountered exception %s', e)
        # If any exception is raised during processing files, a file for this
        # lat/long could not be found
        # we therefore indicate that this was an invalid lat/lon input
        # in my opinion the broad BaseException is acceptable since any
        # processing error will be caused by invalid
        # coordinate inputs (ie file can't be found for this location)
        errors.append("could not process_point: %s" % (e))

    # -----------------------------------------------------
    # Populate the perils using the list from the request
    # -----------------------------------------------------
    objs = {}
    # only add peril to report if it was included in the request peril list
    for peril in cfg['hazards']:
        if peril['name'].upper() in peril_list:
            try:
                objs[peril['name']] = get_peril(peril['name'],
                                                peril['currency'], srcDir,
                                                location.lon, location.lat)
            except PIR_Data_Error as e:
                logger.exception('could not process_point::get peril: %s', e)
                errors.append(peril['name'])
            except BaseException as e:
                logger.exception('Encountered exception %s', e)
                errors.append(peril['name'])


    if len(errors) > 0:
        raise PIR_Data_Error("errors with perils: %s" % ", ".join(errors))

    # date metadata
    currency_time = max([p['currency'] for p in cfg['hazards']])
    updated_time = cfg['date']

    rec = oxml.Record(location, objs, elevation, slope, hail_risk_score,
                      hail_scores, wind_risk_score,
                      transaction, currency_time, updated_time,
                      cfg['contact'], cfg['data_rights'])

    return rec

class PIR_Data_Error(RuntimeError):
    pass