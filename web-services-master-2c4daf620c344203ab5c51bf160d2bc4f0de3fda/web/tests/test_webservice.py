#!/usr/bin/env python

import logging
import json
import unittest

from web.pir_webservice import create_app
import xml.etree.ElementTree as etree
from xml.etree.ElementTree import ParseError
import pkg_resources

API_KEY = 'testing'
INVALID_REQUEST = 400
VALID_REQUEST = 200

SERVER = '/report/'


def get_valid_request():
    return {
        "key": "unittestkey",
        "property_id": "42TX51109143",
        "address": {
            "street": "131 Hartwell Ave",
            "city": "Lexington",
            "state": "MA",
            "postal_code": "02421"
        },
        "coordinates": {
            "latitude": "42.462282",
            "longitude": "-71.267766"
        },
        "hazard_list": [
            "lightning",
            "hail",
            "fire"
        ]
    }


class WebServiceTestCase(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.app = create_app("unittest").test_client()

    @classmethod
    def teardown_class(cls):
        pass

    def test_valid_request(self):

        expected_xml = 'expected.xml'
        xml_stream = pkg_resources.resource_stream('web', expected_xml)

        try:
            # high level test that correct xml is returned
            request = json.dumps(get_valid_request())
            response = self.app.post(SERVER, data=request, content_type='application/json')

            print 'Request: {0}  Response: {1}'.format(request, response.data)

            response_root = etree.fromstring(response.data)
            expected_root = etree.parse(xml_stream).getroot()

            assert response.status_code == VALID_REQUEST
            assert expected_root.tag == "PIR"

            # test request with no location --- geocoding happens
            request = get_valid_request()
            del(request['coordinates'])
            response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')

            print 'Request: {0}  Response: {1}'.format(request, response.data)

            response_root = etree.fromstring(response.data)
            print response_root

            assert response.status_code == VALID_REQUEST
            assert response_root.tag == "PIR"

            # test request with no hazard_list -- hazard_list is optional
            request = get_valid_request()
            del(request['hazard_list'])
            response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')

            print 'Request: {0}  Response: {1}'.format(request, response.data)

            response_root = etree.fromstring(response.data)

            assert response.status_code == VALID_REQUEST
            assert response_root.tag == "PIR"

        except IOError:
            print 'Could not open xml file'
        except ParseError as exc:
            logging.error('Return object was not in the correct format')
            logging.error(response.data)
            logging.error(exc)
            raise AssertionError

    def test_invalid_request(self):
        # test request missing address
        request = get_valid_request()
        del request['address']
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        print response.data
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        self.assertRegexpMatches(resp_json['message']['address'], r'Address object is required', 'Match error message in the response.')

        # test request missing required address field
        request = get_valid_request()
        del request['address']['city']
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        assert resp_json['message'] == "Could not validate json object against required fields: Required fields are missing: city"
        # test request missing required location field
        request = get_valid_request()
        del request['coordinates']['latitude']
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        assert resp_json['message'] == "Could not validate json object against required fields: Required fields are missing: latitude"

        # test badly formed json request
        request = 'this isnt a json'
        response = self.app.post(SERVER, data=request, content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        #assert resp_json['message'] == '400: Bad Request'

        # test invalid lat/long input
        request = get_valid_request()
        request['coordinates']['latitude'] = "-1"
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        assert resp_json['message'] == u'Supplied lat/lon does not correspond to a valid location'

        request['coordinates']['longitude'] = "98"
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        assert resp_json['message'] == u'Supplied lat/lon does not correspond to a valid location'

        # these coordinates are valid but correspond to an invalid location (middle of ocean)
        request = get_valid_request()
        request['coordinates']['latitude'] = "20"
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        assert resp_json['message'] == u'Supplied lat/lon does not correspond to a valid location'

        # test invalid terrain file handling
        request['hazard_list'] = ["lightning", "hail", "terrain"]

        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        assert resp_json['message'] == "Supplied lat/lon does not correspond to a valid location"

        # test invalid hail risk file handling
        request['hazard_list'] = ["lightning", "hail"]
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == INVALID_REQUEST
        assert resp_json['message'] == "Supplied lat/lon does not correspond to a valid location"

    def test_authentication(self):
        request = get_valid_request()
        request['key'] = 'notindb'
        response = self.app.post(SERVER, data=json.dumps(request), content_type='application/json')
        resp_json = json.loads(response.data)
        print 'Request: {0}  Response: {1}'.format(request, resp_json)
        assert response.status_code == 401
