# -*- coding: utf-8 -*-
"""
Created on Thu Jan 22 09:54:40 2015

@author: bwoods

"""
import logging
from datetime import datetime

import xml.etree.ElementTree as ET
import numpy as np

from PIR import __version__


class Annual_Peril:
    '''
    Peril-specific data to be included in the report
    '''

    def __init__(self, year, at_point, neighboring):
        self.year = year
        self.at_point = at_point
        self.neighboring = neighboring

    def __repr__(self):
        return ET.tostring(self.to_XML())

    def to_XML(self, parent=None):
        if parent is not None:
            _year_elem = ET.SubElement(parent, 'Year')
        else:
            _year_elem = ET.Element('Year')

        _yrs_ago = ET.SubElement(_year_elem, 'Years_Ago')
        _yrs_ago.text = unicode(self.year)

        _year_local = ET.SubElement(_year_elem, 'within_1km')
        _year_local.text = unicode(self.at_point)
        _year_region = ET.SubElement(_year_elem, 'within_3km')
        _year_region.text = unicode(self.neighboring)

        return _year_elem


class Last_Event:
    '''
    Dates of the last damaging event for a peril
    '''

    def __init__(self, at_point):
        self.at_point = at_point


class Peril:
    '''
    Date of last incident of each peril
    '''

    def __init__(self, years, median, anomaly, last_event, currency_time):
        '''
        years shoud be a list of the last 5 years
        '''
        if len(years) < 5:
            raise ValueError('Must provide at least last 5 years')

        # ensure that years are sorted newest to oldest
        yrs = np.array([y.year for y in years])
        if not np.all(np.diff(yrs) == 1):
            raise ValueError('Years must be subsequent and sorted most recent '
                             'to oldest. Provided: %s', yrs)

        self.years = years
        self.median = median
        self.anomaly = anomaly

        self.last_event = last_event

        self.currency_time = currency_time

    def __repr__(self):
        return ET.tostring(self.to_XML())

    def to_XML(self, peril_key, parent=None):
        '''
            Create the SubElements in *root* named *peril_key* from class
            member *pobj*
            '''
        # SubElements for each peril (per year)
        if parent is not None:
            pelem = ET.SubElement(parent, peril_key)
        else:
            pelem = ET.Element(peril_key)

        _currency = ET.SubElement(pelem, 'Currency_Date')
        _currency.text = self.currency_time.strftime(_dateFmt)

        _median = ET.SubElement(pelem, 'median')
        _median.text = unicode(self.median)
        _anomaly = ET.SubElement(pelem, 'anomaly')
        _anomaly.text = unicode(np.round(self.anomaly).astype(int))

        _elem_last = ET.SubElement(pelem, 'Last_Event')
        _elem_last.text = unicode(self.last_event.at_point)

        elem_years = ET.SubElement(pelem, 'Yearly_counts')
        for year_record in self.years:
            year_record.to_XML(elem_years)

        return pelem


class Location:
    '''
    Location for query
    '''

    def __init__(self, propertyID, address, address2, city, state, zipcode,
                 lat=None, lon=None):
        self.propertyID = propertyID
        self.address = address
        self.address2 = address2
        self.city = city
        self.state = state
        self.zipcode = zipcode
        self.lat = lat
        self.lon = lon

    def __repr__(self):
        return ET.tostring(self.to_XML())

    def to_XML(self, parent=None):
        '''
        Convert to XML eTree
        '''
        if parent is not None:
            _loc_elem = ET.SubElement(parent, 'Location')
        else:
            _loc_elem = ET.Element('Location')

        _property_elem = ET.SubElement(_loc_elem, 'Property_ID')
        _property_elem.text = unicode(self.propertyID)
        _address_elem = ET.SubElement(_loc_elem, 'Address')
        _address_elem.text = unicode(self.address)
        _address2_elem = ET.SubElement(_loc_elem, 'Address2')
        _address2_elem.text = unicode(self.address2)
        _city_elem = ET.SubElement(_loc_elem, 'City')
        _city_elem.text = unicode(self.city)
        _state_elem = ET.SubElement(_loc_elem, 'State')
        _state_elem.text = unicode(self.state)
        _zip_elem = ET.SubElement(_loc_elem, 'Zip_Code')
        _zip_elem.text = unicode(self.zipcode)
        _lat_elem = ET.SubElement(_loc_elem, 'Latitude')
        _lat_elem.text = unicode(self.lat)
        _lon_elem = ET.SubElement(_loc_elem, 'Longitude')
        _lon_elem.text = unicode(self.lon)

        return _loc_elem


class Hail_Damage_Score:
    '''
    score is the max hail probability that year
    events is a count of events > 50% hail prob
    '''

    def __init__(self, year, score, events):
        self.year = year
        self.score = score
        self.events = events

    def __repr__(self):
        return ET.tostring(self.to_XML())

    def to_XML(self, parent=None):
        '''
        Attach data to XML eTree
        '''
        if parent is not None:
            _hds_elem = ET.SubElement(parent, 'Year')
        else:
            _hds_elem = ET.Element('Year')

        _yrs_ago = ET.SubElement(_hds_elem, 'Years_Ago')
        _yrs_ago.text = unicode(self.year)

        _score = ET.SubElement(_hds_elem, 'score')
        _score.text = unicode(self.score)
        _events = ET.SubElement(_hds_elem, 'events')
        _events.text = unicode(self.events)

        return _hds_elem


class Hail_Damage:
    '''
    All of the hail damage scores
    '''

    def __init__(self, score_list, currency_time):
        self.score_list = score_list
        self.currency_time = currency_time

    def __repr__(self):
        return ET.tostring(self.to_XML())

    def to_XML(self, parent=None):
        '''
        Attach data to XML eTree
        '''
        if parent is not None:
            _currency_elem = ET.SubElement(parent, 'Currency_Date')
        else:
            _currency_elem = ET.Element('Currency_Date')

        _currency_elem.text = self.currency_time.strftime(_dateFmt)

        for score in self.score_list:
            score.to_XML(parent)

        return _currency_elem


class Record:
    '''
    PIR record for delivery
    '''

    def __init__(self, geocode, perils, elevation, slope,
                 hail_risk, hail_damage, wind_risk, transaction,
                 currency_time, updated_time, contact, data_rights,
                 query_time=datetime.now()):
        self.geocode = geocode
        self.perils = perils

        # static terrain info
        self.elevation = elevation
        self.slope = slope

        # static risk score
        self.hail_risk = hail_risk
        self.wind_risk = wind_risk

        # annual damage scores
        self.hail_damage = hail_damage

        self.transaction = transaction
        self.query_time = query_time
        self.currency_time = currency_time
        self.updated_time = updated_time
        self.contact = contact
        self.data_rights = data_rights

    def __repr__(self):
        return ET.tostring(self.to_XML())

    def to_XML(self):
        '''
        Convert the Record instance to an XML ElemntTee
        '''
        # we start with a root tag
        root = ET.Element(_root_key)

        peril_key_dict = {'Fire': _fire_key, 'Lightning': _lightning_key,
                          'Hail': _hail_key, 'Wind': _wind_key}

        # create SubElements for each peril
        for key, peril in self.perils.iteritems():
            peril.to_XML(peril_key_dict[key], parent=root)

        # only include terrain and hail risk information if included in the
        # request surface data
        if self.elevation:
            _terrain = ET.SubElement(root, _terrain_key)
            _slope = ET.SubElement(_terrain, 'slope_code')
            _slope.text = unicode(self.slope)
            _elevation = ET.SubElement(_terrain, 'elevation')
            _elevation.text = unicode(np.round(self.elevation).astype(int))

        # Hail Risk Score
        if self.hail_risk:
            _hail_risk = ET.SubElement(root, _hail_risk_key)
            _hail_risk.text = unicode(self.hail_risk)

        # Hail Risk Score
        if self.wind_risk:
            _wind_risk = ET.SubElement(root, _wind_risk_key)
            _wind_risk.text = unicode(self.wind_risk)

        # Hail Damage Scores (per year)
        if self.hail_damage:
            _hail_damage = ET.SubElement(root, _hail_damage_key)
            self.hail_damage.to_XML(parent=_hail_damage)

        _transaction = ET.SubElement(root, 'Transaction')
        _transaction.text = unicode(self.transaction)

        _query_time = ET.SubElement(root, 'Query_Time')
        _query_time.text = unicode(self.query_time)
        _current_time = ET.SubElement(root, 'Currency_Date')
        _current_time.text = self.currency_time.strftime(_dateFmt)
        _update_time = ET.SubElement(root, 'Update_Date')
        _update_time.text = self.updated_time.strftime(_dateFmt)

        # Location of query
        self.geocode.to_XML(parent=root)

        _rights_elem = ET.SubElement(root, 'data_rights')
        _rights_elem.text = self.data_rights

        _version_elem = ET.SubElement(root, 'version')
        _version_elem.text = __version__

        _contact_elem = ET.SubElement(root, 'contact')
        _contact_elem.text = self.contact

        return root

# global date format
_dateFmt = '%Y-%m-%d'

_root_key = 'PIR'
# keys for XML sections
_hail_key = 'Hail_Events'
_wind_key = 'Wind_Events'
_lightning_key = 'Lightning_Events'
_fire_key = 'Wildfire_Events'
_terrain_key = 'Terrain'
_hail_risk_key = 'Hail_Risk_Score'
_wind_risk_key = 'Wind_Risk_Score'
_hail_damage_key = 'Hail_Damage_Score'
