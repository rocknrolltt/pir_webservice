#!/usr/bin/env python

from flask import Flask, g, got_request_exception, request
from pir_resources import PirService
from flask_login import LoginManager
from database.pir_user import request_user
from logging import config
from pir_resources import invalid_response
from datetime import datetime
import argparse
import flask_restful as restful
import pkg_resources
import logging
import re

# app config
SECRET_KEY = '03f8c292-6388-4cab-ab3c-91621cee3fda'
VERSION = '0.4.1'
SERVICE_URL = '/report/'

logger = logging.getLogger("pirweb")


def log_exception(sender, exception, **extra):
    '''
    whenever an exception occurs during the request (authentication error, missing field, etc), return a response
    with correct response code
    '''
    sender.logger.error('Got an exception during processing: {0}'.format(exception))
    codes = re.findall(r'\d+', str(exception))
    logger.error('Error codes from exception {0}{1}'.format(exception, codes))

    # check if exception had an associated error code, if not log it as an internal exception
    code = 500
    if codes:
        code = int(codes[0])

    request_json = None

    try:
        request_json = request.json
    except Exception as e:
        # if exception is throw when trying to access request.json then the request was badly formed (not a json)
        logger.error('Not a json request: {0}'.format(e))
    finally:
        if not request_json:
            request_json = {}

        request_json['remote_ip'] = request.remote_addr
        return invalid_response(code, str(exception), request_json, 0)


def create_app(credentials):
    '''
    creates Flask application and adds config settings and the restful api
    we also include a LoginManager and associate a request loader with it

    :param credentials: this is a tag name for the database configuration section
    '''

    web_app = Flask(__name__)
    web_app.config.from_object(__name__)
    web_app.config.from_envvar('PIR_SETTINGS', silent=True)

    api = restful.Api(web_app)
    api.add_resource(PirService, SERVICE_URL)

    login_manager = LoginManager()
    login_manager.request_loader(request_user)
    login_manager.init_app(web_app)

    web_app.config['DATABASE_CREDENTIALS'] = credentials

    logger.debug(
        'URL: {0}, Version: {1}, Database Credentials: {2}'.format(SERVICE_URL, VERSION,
                                                                   web_app.config['DATABASE_CREDENTIALS']))

    got_request_exception.connect(log_exception, web_app)

    # closes the database connection when we don't need it anymore (after each http request)
    @web_app.teardown_appcontext
    def close_db(self):
        if hasattr(g, 'sqlite_db'):
            logger.debug('Closing database instance')
            g.sqlite_db.close()

    # set start time of request
    @web_app.before_request
    def set_start_time():
        g.start = datetime.now()

    # log how long the request took (called after every request)
    @web_app.after_request
    def calculate_response_time(response):
        logger.info('Time to return response: {0}'.format(datetime.now() - g.start))
        return response

    return web_app


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true',
                        help="Dev flag indicates if we use dev configs")
    parser.add_argument('-p', '--port', default=24602, dest='port',
                        type=int, help='Port on which to run.')
    parser.add_argument('--hostconfig', default='dev', dest='hostconfig',
                        help='flag for external visibility of the standalone web service (dev|toExternal)')
    args = parser.parse_args()

    if args.dev:
        log_config = 'dev_log.cfg'
    else:
        log_config = 'prod_log.cfg'

    log_config = 'dev_log.cfg'
    cfg_stream2 = pkg_resources.resource_string('web', log_config)
    print cfg_stream2
    print pkg_resources.resource_filename('web', log_config)
    cfg_stream = pkg_resources.resource_stream('web', log_config)
    print cfg_stream
    logging.config.fileConfig(cfg_stream, disable_existing_loggers=False)

    hostcfg = '127.0.0.1'
    if args.hostconfig == 'toExternal':
        hostcfg = '0.0.0.0'

    if args.dev:
        db_credentials = 'dev_test'
    else:
        db_credentials = 'production'

    app = create_app(db_credentials)
    logger.debug('Running app with {0}'.format(log_config))
    app.run(host=hostcfg, port=args.port)
