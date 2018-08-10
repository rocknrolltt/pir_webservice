#!/usr/bin/env python

from flask_login import UserMixin
from web.database import manage_db
from flask_restful import reqparse
from flask import current_app
import logging

logger = logging.getLogger("PIRWeb")


class PirUser(UserMixin):
    """
    Class representation of a user contained in our db file.
    Simple representation includes user name and api key
    UserMixin provides necessary default methods required by LoginManager
    """
    def __init__(self, name, key):
        self.key = key
        self.name = name

    # method takes a key
    # attempts to get user from db, returns None if no user found
    @classmethod
    def get_user(cls, key, credentials):
        user = manage_db.get_user_by_key(key, credentials)

        if not user:
            logger.info('Key was not found in the database')
            return None

        return cls(*user)


def request_user(request):
    """
    This is our request handler method for authentication. Method will try to
    authenticate using the api key first, and Basic Authorization as a second
    option. If it can't authenticate, we return None and flask will throw a 401
    exception. Method that need authentication should use the login_required decorator
    in order for this method to be called
    """
    # add requirement that key is included in request. Reqparse will return bad request if key not included
    parser = reqparse.RequestParser()
    parser.add_argument('key', type=str, required=True, help="API key is required", location='json')

    # get key from request
    args = parser.parse_args()
    api_key = args['key']
    credentials = current_app.config['DATABASE_CREDENTIALS']

    logger.debug('Using credentials: {0}'.format(credentials))

    # if user can be found with api key
    # authentication is successful and we return the user
    # otherwise we return None and flask-restful understands authentication has failed

    user = PirUser.get_user(api_key, credentials)
    if user:
        logger.info('API key successfully authenticated')
        return user

    logger.info('API key could not be authenticated')

    return None


