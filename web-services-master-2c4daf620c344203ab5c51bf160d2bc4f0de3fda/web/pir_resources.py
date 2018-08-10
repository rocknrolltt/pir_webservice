#!/usr/bin/env python

from flask_restful import reqparse, Resource
from flask_login import login_required
from flask import make_response, request, jsonify, g
from geocode import geocode_main
from uuid import uuid4
from PIR.output_xml import Location
from PIR.load_point import process_point, PIR_Data_Error
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, time
from sqlconmanager.connection_manager import Manager, ConnectionLevel, ManagerConnectionException
import pkg_resources
import logging
import yaml
import json


ALL_HAZARDS = ['Lightning', 'Hail', 'Fire', 'Wind', 'Hail Risk', 'Terrain']

OK = 200
INTERNAL_ERROR = 500
INVALID_REQUEST = 400
AUTH_ERROR = 401
INVALID_ADDRESS = 402

# min/max for latitudes and longitudes. Taken from tif files
LONG_MIN = -130
LONG_MAX = -60
LAT_MIN = 20.0
LAT_MAX = 55.0049993

logger = logging.getLogger("pirweb")
trans_logger = logging.getLogger("trans")
con_mgr = Manager()


def log_transaction(trans_id, request_json, result_code, geocode_status, message):
    '''log a transaction using the generated uuid as primary key'''
    return_time_delta = datetime.now() - g.start
    return_secs = return_time_delta.seconds

    try:
        # if transaction failed we might not yet have the coordinates/hazard list set (they are not required)
        # indicate in db if they were not set
        if 'coordinates' in request_json and 'latitude' in request_json['coordinates'] and 'longitude' in \
                request_json['coordinates']:
            lat = request_json['coordinates']['latitude']
            lon = request_json['coordinates']['longitude']
        else:
            lat = -999.9
            lon = -999.9

        if 'hazard_list' in request_json:
            hazards = str(request_json['hazard_list'])
        else:
            hazards = None

        log_json = {
            "risk_id": request_json['property_id'],
            "street": request_json['address']['street'],
            "city": request_json['address']['city'],
            "state": request_json['address']['state'],
            "postal_code": request_json['address']['postal_code'],
            "latitude": lat,
            "longitude": lon,
            "hazards": hazards,
            "trans_id": str(trans_id),
            "geocode_status": geocode_status,
            "result_code": str(result_code),
            "trans_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z"),
            "remote_ip": request_json['remote_ip'],
            "message": message,
            "return_seconds": return_secs
        }

        trans_logger.info(json.dumps(log_json))
        logger.info('Successfully cached transaction {0} with code {1}'.format(trans_id, result_code))

    except (KeyError, TypeError) as myerror:
        logger.error('Field was missing. (%s) Caching only trans id, geocode status, and result http code', myerror)

        log_json = {
            "risk_id": None,
            "street": None,
            "city": None,
            "state": None,
            "postal_code": None,
            "latitude": None,
            "longitude": None,
            "hazards": None,
            "trans_id": str(trans_id),
            "geocode_status": geocode_status,
            "result_code": str(result_code),
            "trans_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z"),
            "remote_ip": request_json['remote_ip'],
            "message": message,
            "return_seconds": return_secs
        }

        trans_logger.info(json.dumps(log_json))
        logger.info('Successfully cached transaction {0} with code {1}'.format(trans_id, result_code))



def invalid_response(code, msg, request_json, geocoded):
    """
        make a flask response with given code and message
        used if error occurs or request was invalid
        trans_id is not created when error occurs so create one now
    """
    logger.debug('Returning failure response with code: {0} and message: {1}'.format(code, msg))
    transaction_id = uuid4()
    logger.debug('Created id for transaction: {0}'.format(transaction_id))
    log_transaction(transaction_id, request_json, code, geocoded, msg)
    logger.debug("make invalid response: { trans_id:%s, message=%s, status=%s }", transaction_id, msg, code)

    response = make_response(jsonify(trans_id=transaction_id, message=msg, status=code), code,
                         {'Content-Type': 'application/json'})

    logger.debug("response: %s", response)
    logger.debug("response.data: %s", response.data)

    return make_response(jsonify(trans_id=transaction_id, message=msg, status=code), code,
                         {'Content-Type': 'application/json'})


class InvalidRequestException(Exception):
    pass


class PirService(Resource):
    """ Flask-restful resource handler for requests on specified server"""

    def __init__(self):
        # reqparse is a module which verifies that the request has these specified attributes
        # if attribute doesn't exists a 400 error code will be returned with the help message
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('property_id', type=str, required=True, help='Property ID is required',
                                 location='json')
        self.parser.add_argument('address', required=True, help='Address object is required',
                                 location='json')
        self.parser.add_argument('coordinates', help='Supplied coordinates of completed geocode',
                                 location='json')
        self.parser.add_argument('hazard_list', help='List of hazards to include in report',
                                 location='json')

    def _validate_json(self, required_fields, to_check):
        '''
        Check that a json object has the required fields
        we first get a list of all observed fields in the object, and compare it
        with the passed required fields. InvalidRequestException is raised if
        a required field is not present

        :param require_fields: list of required keys
        :param to_check: dictionary to be validated
        '''

        try:
            observed_fields = []
            missing_fields = []
            for key, value in to_check.iteritems():
                observed_fields.append(key)

            for required in required_fields:
                if required not in observed_fields:
                    logger.error('{0} is a required field but was missing from the request'.format(required))
                    missing_fields.append(required)

            # if there are any missing fields, this is an invalid request
            if missing_fields:
                raise InvalidRequestException('Required fields are missing: {0}'.format(','.join(missing_fields)))

        except AttributeError:
            # can be raised if a to_check value is None, for example
            raise InvalidRequestException('Could not validate Json. Key value is invalid')
        except Exception as e:
            # catch all other exceptions raised while validating json...if exception is raised, json is badly formed
            # and we return an error indicating this
            raise InvalidRequestException('Could not validate json object against required fields: {0}'.format(e))

    def _validate_hazards(self, hazards):
        ''' Check that the hazard inputs in the hazard request object are valid'''
        supported_hazards = ['WIND', 'FIRE', 'HAIL', 'LIGHTNING', 'TERRAIN']
        unsupported = []

        for hazard in hazards:
            if hazard.upper() not in supported_hazards:
                logger.error('{0} is not a supported hazard type'.format(hazard))
                unsupported.append(hazard.upper())

        if unsupported:
            error_msg = 'Unsupported hazard types included in request: {0}'.format(', '.join(unsupported))
            if 'WILDFIRE' in unsupported:
                error_msg += ' (try FIRE instead of WILDFIRE)'

            raise InvalidRequestException(error_msg)

    def _validate_coordinates(self, lat, lon):
        '''Check that coordinate input is valid and able to be converted to a float'''
        try:

            latitude = float(lat)
            longitude = float(lon)

            if not LONG_MIN <= longitude <= LONG_MAX:
                raise InvalidRequestException('Supplied lat/lon does not correspond to a valid location')

            if not LAT_MIN < latitude <= LAT_MAX:
                raise InvalidRequestException('Supplied lat/lon does not correspond to a valid location')

        except ValueError:
            error_msg = 'Invalid lat/long input'
            raise InvalidRequestException(error_msg)
        except TypeError:
            raise InvalidRequestException('Invalid lat/long -- Could not convert to float value')

    def _create_loc_object(self, req_json):
        address1 = req_json['address']['street']
        address2 = ''
        city = req_json['address']['city']
        state = req_json['address']['state']
        postal = req_json['address']['postal_code']

        # we can make this conversion without error checking, since we already validated the json request in
        # _validate_coordinates
        lat = float(req_json['coordinates']['latitude'])
        lon = float(req_json['coordinates']['longitude'])

        return Location(req_json['property_id'], address1, address2, city, state, postal, lat, lon)

    def _log_geocoded_address(self, request_json):
        try:
            config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')
            engine = con_mgr.get_connection(config_stream, security_level=ConnectionLevel.UPDATE, sql_echo=True)

            risk_id = request_json['property_id']
            street = request_json['address']['street']
            city = request_json['address']['city']
            state = request_json['address']['state']
            postal = request_json['address']['postal_code']
            lat = request_json['coordinates']['latitude']
            lon = request_json['coordinates']['longitude']

            engine.prometrix_addresses.insert(riskid=risk_id, streetname=street, city=city, statecode=state,
                                              zip=postal, lat=lat, long=lon)

            engine.commit()
            logger.info('Successfully cached geocoded address with property id {0}'.format(risk_id))
        except SQLAlchemyError as e:
            logger.error(str(e))

    def _get_cached_address(self, request_json):
        try:
            config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')
            engine = con_mgr.get_connection(config_stream)

            risk_id = request_json['property_id']

            result = engine.prometrix_addresses.filter_by(riskid=risk_id).one()

            return result
        except SQLAlchemyError as e:
            logger.error(str(e))
        except ManagerConnectionException as e:
            logger.error("Could not connect to database server. Logging failed. {0}".format(e))

    @login_required
    def post(self):
        """Handles post request"""

        # get the json request object, we don't need the key since authentication has already
        # happened at this point
        # 0 if no geocoding, 1 if geocoding occurred
        geocoded = 0

        self.parser.parse_args()

        req_json = request.json
        remote_address = request.remote_addr
        address = req_json['address']
        coordinates = None

        logger.debug('Request: {0}'.format(request.data))
        req_json['remote_ip'] = remote_address

        request_keys = []
        address_required = ['street', 'city', 'state', 'postal_code']
        coordinates_required = ['latitude', 'longitude']
        possible_keys = ['key', 'property_id', 'address', 'coordinates', 'hazard_list', 'remote_ip']

        # need a list of upper most keys in our json object in order to check requirements
        # if request has unexpected keys, then we return an invalid_request response
        unexpected_keys = []
        for key in request.json:
            if key not in possible_keys:
                unexpected_keys.append(key)

            request_keys.append(key)

        # return invalid response if there are unexpected keys in the json request
        if unexpected_keys:
            unexpected_string = ', '.join(unexpected_keys)
            error_msg = 'Unexpected keys ({0}) present in request'.format(unexpected_string)
            logger.error('JSON request contained unexpected keys: {0}'.format(unexpected_string))

            return invalid_response(INVALID_REQUEST, error_msg, req_json, geocoded)

        # validate our address/coordinates/hazards objects, if validation unsuccessful we have an invalid request
        # so we return an invalid response -- required field was missing.
        # only check coordinates if it was supplied in the request
        try:
            self._validate_json(address_required, address)
            if "coordinates" in request_keys:
                self._validate_json(coordinates_required, req_json['coordinates'])
                self._validate_coordinates(req_json['coordinates']['latitude'], req_json['coordinates']['longitude'])
                coordinates = req_json['coordinates']

            if 'hazard_list' in request_keys:
                self._validate_hazards(req_json['hazard_list'])
            else:
                logger.debug('No hazard list passed, using all hazards')
                req_json['hazard_list'] = ALL_HAZARDS
        except InvalidRequestException as e:
            return invalid_response(INVALID_REQUEST, e.message, req_json, geocoded)

        # if no coordinates were supplied, check if address is included in cache. if not we must geocode.
        # Call the geocoder on address request object
        # alter our request object with geocoder response...this will be then passed to file pulling module
        # if coordinates provided, we use these coordinates and the supplied address for file processing
        if not coordinates:
            # cached = self._get_cached_address(req_json)
            cached = None
            if cached:
                logger.info('Found cached address: {0}'.format(cached))
                address['street'] = cached.streetname
                address['city'] = cached.city
                address['state'] = cached.statecode
                address['postal_code'] = cached.zip
                coordinates = {'latitude': cached.lat, 'longitude': cached.long}
                req_json['coordinates'] = coordinates
            else:
                logger.info('Trying to geocode address {0}'.format(address))
                geo_response = geocode_main.geocode(address['street'], address['city'], address['state'],
                                                    address['postal_code'], [geocode_main.GOOGLE_GEO, geocode_main.ISO_PRODUCTION])
                logger.info('Response: {0}'.format(geo_response))
                if not geo_response['error_msg']:
                    address['street'] = geo_response['street']
                    address['city'] = geo_response['city']
                    address['state'] = geo_response['state_prov']
                    address['postal_code'] = geo_response['postal_code']
                    address['country'] = geo_response['country']
                    coordinates = {'latitude': geo_response['latitude'], 'longitude': geo_response['longitude']}
                    req_json['coordinates'] = coordinates
                    geocoded = 1
                    # self._log_geocoded_address(req_json)
                else:
                    logger.error('Geocoders failed to find this address')
                    return invalid_response(INVALID_ADDRESS, 'Geocoder could not find address', req_json, geocoded)

        # create a new transaction id and request location object
        transaction_id = uuid4()
        logger.debug('Created id for transaction: {0}'.format(transaction_id))
        req_location = self._create_loc_object(req_json)

        # load configuration data about datasets and call function to extract xml for the request address
        cfg_file = pkg_resources.resource_stream('PIR', 'etc/datasets.cfg')
        cfg = yaml.load(cfg_file)
        try:
            self._validate_coordinates(req_json['coordinates']['latitude'], req_json['coordinates']['longitude'])
            result = process_point(req_location, transaction_id, cfg, req_json['hazard_list'])
        except InvalidRequestException as ex:
            return invalid_response(INVALID_REQUEST, e.message, req_json, geocoded)
        except PIR_Data_Error as ex:
            logger.error("could not process point for %s. DATA ERROR: %s", transaction_id, ex)
            return invalid_response(INTERNAL_ERROR, "Internal Data Error", req_json, geocoded)


        # if we could not get a result from these coordinates then they correspond to an invalid location
        if not result:
            error_msg = "Supplied lat/lon does not correspond to a valid location"
            return invalid_response(INVALID_REQUEST, error_msg, req_json, geocoded)

        #logger.debug('File extraction result: {0}'.format(result))
        log_transaction(transaction_id, req_json, OK, geocoded, "200: Successful transaction")
        return make_response(unicode(result), OK, {'Content-Type': 'application/xml'})
