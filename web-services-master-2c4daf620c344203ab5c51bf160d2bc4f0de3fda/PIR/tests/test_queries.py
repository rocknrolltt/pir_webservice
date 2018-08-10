# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 11:00:02 2015

@author: bwoods
"""
from nose.tools import assert_equal, assert_not_equal
from nose.tools import raises

import pandas as pd
import yaml
import random
import xmltodict
import os
import xml.etree.ElementTree as etree
from xml.dom import minidom
import logging

import PIR
import PIR.output_xml as oxml
from PIR.load_point import process_point

def _prettify_xml(doctext):
    # Prettify the given XML string
    reparsed = minidom.parseString(doctext)
    return reparsed.toprettyxml(indent='  ')


def _unidiff_output(expected, actual):
    """
    Helper function. Returns a string containing the unified diff of two
    multiline strings.
    """

    import difflib
    expected = expected.splitlines(1)
    actual = actual.splitlines(1)
    diff = difflib.unified_diff(expected, actual)

    return ''.join(diff)


class Test_Queries(object):
    @classmethod
    def setup_class(cls):
        '''
        This method is run once for each class before any tests are run
        '''
        # read in the example queries to validate the results
        xlsx_path = os.path.abspath(os.path.join(__file__,
                                    '../TestAddresses.xlsx'))
        cls.df = pd.io.excel.read_excel(xlsx_path)
        # we'll need a config file for the queries
        cfg_path = os.path.abspath(os.path.join(__file__,
                                   '../../etc/datasets.cfg'))
        with open(cfg_path, 'r') as f:
            cls.cfg = yaml.load(f)

        cls.peril_list = ['Hail Risk', 'Terrain', 'Hail', 'Wind', 'Lightning',
                          'Fire']

    @classmethod
    def teardown_class(cls):
        '''
        This method is run once for each class _after_ all tests are run
        '''
        pass

    def setup(self):
        '''
        This method is run once before _each_ test method is executed
        '''
        self.test_mode = "REGRESSION"

    def teardown(self):
        '''
        This method is run once after _each_ test method is executed
        '''
        pass

    def test_DEM(self):
        '''
        Test that the proper DEM elevation is being found
        '''
        # query some geocoder DEM
        # query out GeoTIFFs
        # find the difference and assert that it is sane
        pass

    @raises
    def test_invalid_coords(self):
        '''
        Test that the functionality errors for invalid coordinates
        '''
        row = self.df[0]
        property_id = 'Test_raises'
        location = oxml.Location(property_id, row['street'], '', row['city'],
                                 row['state'], row['postal_cod'],
                                 lat=91, lon=-90)
        transid = "%032x" % random.getrandbits(128)
        process_point(location, transid, self.cfg, self.peril_list)

    @raises
    def test_invalid_domain(self):
        '''
        Test that the functionality errors for coordinates out of domain
        '''
        row = self.df[0]
        property_id = 'Test_raises'
        location = oxml.Location(property_id, row['street'], '', row['city'],
                                 row['state'], row['postal_cod'],
                                 lat=-40, lon=-90)
        transid = "%032x" % random.getrandbits(128)
        process_point(location, transid, self.cfg, self.peril_list)

    def test_sample_report(self):
        '''
        Generate a report and verify it matches expected template
        '''
        # generate the query
        transid = '1234567890'
        location = oxml.Location('Null', '474 Central Ave', '',
                                 'Highland Park', 'IL', '60035-2680',
                                 lat=42.185879, lon=-87.797329)
        query = str(process_point(location, transid, self.cfg,
                                  self.peril_list))
        # we are provided the expected result
        xml_file = os.path.abspath(os.path.join(__file__, '../Illinois.xml'))
        with open(xml_file) as f:
            template = str(f.read())

        # assert that XML files are the same
        obj1 = etree.fromstring(template)
        obj2 = etree.fromstring(query)

        result = _prettify_xml(etree.tostring(obj2))
        # strings should be different based on query time and maybe version
        assert_not_equal(template, result)

        # set the query times equal
        tag_names = ['Query_Time', 'version', 'data_rights', 'contact']
        for tag in tag_names:
            _t1 = obj1.find(tag)
            _t2 = obj2.find(tag)
            # copy the value to the object
            _t2.text = _t1.text

        result = _prettify_xml(etree.tostring(obj2))
        # now the strings should be equal
        logging.debug("here is the result: %s", result)
        logging.debug("here is the template: %s", template)

        assert_equal(template, result, _unidiff_output(template, result))

    def get_year_count(self, yr_obj, year, key):
        '''
        Get year of XML *yr_obj* with *key* matches *val*
        '''
        for yr in yr_obj:
            if yr['Years_Ago'] == unicode(year):
                return yr[key]
        return None

    def test_all_rows(self):
        '''
        Test the query results for each row in the test spreadsheet
        '''
        for index, row in self.df.iterrows():
            # process the query for that address
            property_id = 'Test_%i' % index
            location = oxml.Location(property_id, row['street'], '',
                                     row['city'], row['state'],
                                     row['postal_cod'],
                                     lat=row['latitude'], lon=row['longitude'])
            transid = "%032x" % random.getrandbits(128)
            result = process_point(location, transid, self.cfg,
                                   self.peril_list)

            # parse the returned XML
            root = xmltodict.parse(str(result))['PIR']
            print 'Testing', location
            # extract the annual peril count list
            hail_years = root['Hail_Events']['Yearly_counts']['Year']

            # make sure the address info matches
            loc = root['Location']
            assert_equal(property_id, loc['Property_ID'])
            assert_equal(unicode(row['street']), loc['Address'])
            assert_equal(None, loc['Address2'])
            assert_equal(unicode(row['city']), loc['City'])
            assert_equal(unicode(row['state']), loc['State'])
            assert_equal(unicode(row['postal_cod']), loc['Zip_Code'])
            assert_equal(unicode(row['latitude']), loc['Latitude'])
            assert_equal(unicode(row['longitude']), loc['Longitude'])

            assert_equal(PIR.__version__, root['version'])

            if self.test_mode == "TESTING":
                assert_equal(self.get_year_count(hail_years, 1, 'within_1km'),
                             unicode(row['H_YTD1km']), "error in 'within_1km Hail' for row %s"%index)
                assert_equal(self.get_year_count(hail_years, 1, 'within_3km'),
                             unicode(row['H_YTD3km']), "error in 'within_3km Hail' for row %s"%index)

                wind_years = root['Wind_Events']['Yearly_counts']['Year']
                assert_equal(self.get_year_count(wind_years, 1, 'within_1km'),
                             unicode(row['W_YTD1km']), "error in 'within_1km Wind' for row %s"%index)
                assert_equal(self.get_year_count(wind_years, 1, 'within_3km'),
                             unicode(row['W_YTD3km']), "error in 'within_3km Wind' for row %s"%index)

                fire_years = root['Wildfire_Events']['Yearly_counts']['Year']
                assert_equal(self.get_year_count(fire_years, 1, 'within_1km'),
                             unicode(row['F_YTD1km']), "error in 'within_1km Wildfire' for row %s"%index)
                assert_equal(self.get_year_count(fire_years, 1, 'within_3km'),
                             unicode(row['F_YTD3km']), "error in 'within_3km Wildfile' for row %s"%index)

                lightning_years = root['Lightning_Events']['Yearly_counts']['Year']
                assert_equal(self.get_year_count(lightning_years, 1, 'within_1km'),
                             unicode(row['L_YTD1km']), "error in 'within_1km Lightning' for row %s"%index)
                assert_equal(self.get_year_count(lightning_years, 1, 'within_3km'),
                             unicode(row['L_YTD3km']), "error in 'within_3km Lightning' for row %s"%index)
            elif self.test_mode == "REGRESSION":
                wind_years = root['Wind_Events']['Yearly_counts']['Year']
                fire_years = root['Wildfire_Events']['Yearly_counts']['Year']
                lightning_years = root['Lightning_Events']['Yearly_counts']['Year']
                # F_YTD1km	F_YTD3km	H_YTD1km	H_YTD3km	L_YTD1km	L_YTD3km	W_YTD1km	W_YTD3km
                print ("{property_id},{address},{city},{state},{postal_code},{latitude},{longitude},"
                       "{fire_ytd_1km},{fire_ytd_3km},{hail_ytd_1km},{hail_ytd_3km},"
                       "{lightning_ytd_1km},{lightning_ytd_3km},{wind_ytd_1km},{wind_ytd_3km}").format(
                        property_id=loc['Property_ID'],
                        address=loc['Address'],
                        city=loc['City'],
                        state=loc['State'],
                        postal_code=loc['Zip_Code'],
                        latitude=loc['Latitude'],
                        longitude=loc['Longitude'],
                        fire_ytd_1km=self.get_year_count(fire_years, 1, 'within_1km'),
                        fire_ytd_3km=self.get_year_count(fire_years, 1, 'within_3km'),
                        hail_ytd_1km=self.get_year_count(hail_years, 1, 'within_1km'),
                        hail_ytd_3km=self.get_year_count(hail_years, 1, 'within_3km'),
                        lightning_ytd_1km=self.get_year_count(lightning_years, 1, 'within_1km'),
                        lightning_ytd_3km=self.get_year_count(lightning_years, 1, 'within_3km'),
                        wind_ytd_1km=self.get_year_count(wind_years, 1, 'within_1km'),
                        wind_ytd_3km=self.get_year_count(wind_years, 1, 'within_3km'))

