#!/usr/bin/env python

from sqlite3 import dbapi2 as sqlite3, IntegrityError, OperationalError
import uuid
from flask import g
import logging
import pkg_resources
import yaml

logger = logging.getLogger("PIRWeb")
config_level = 'production'


class NoDatabaseException(Exception):
    pass


def query_db(query, credentials, args=(), one=False):
    """
    this is our query function, it gets a db and executes the provided query
    :param query: Sql query string for desired query
    :param args: arguments for ? placeholders in sql string -- prevents sql injection
    :param one: one is set to true if we only want one result of query back
    :return: result of query
    """
    try:
        logger.debug('Executing the following sql: {0}'.format(query))
        cur = get_db(credentials).execute(query, args)
        rv = cur.fetchall()
    except OperationalError as error:
        logger.error('Problem with database query: {0}'.format(error))
        return None
    except NoDatabaseException:
        return None

    return (rv[0] if rv else None) if one else rv


def connect_db(credentials):
    """open a new sqlite3 connection"""

    config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')
    db_config = yaml.load(config_stream)

    logger.debug('Using {0} credentials to connect to database'.format(credentials))

    database = db_config['user_database_configurations'][credentials]['path']

    # first check if the specified database exists. if it doesnt, we raise an exception,
    # which is caught in the query_db function
    try:
        with open(database):
            logger.debug('Database {0} exists'.format(database))
    except IOError:
        logger.error('Database {0} does not exist'.format(database))
        raise NoDatabaseException

    rv = sqlite3.connect(database)
    # converts db return to dictionary
    rv.row_factory = sqlite3.Row
    return rv


def get_db(credentials):
    """
    check if flask app has the database instance already loaded
    if not we must first make the connection and then set the flask variable to point to it
    *note flask.g is a special object that is valid for an active request and is intended to hold
    values that need to be held throughout the entire request
    :return: flask application database instance
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db(credentials)
    return g.sqlite_db


def get_user_by_key(key, credentials):
    """validate user by key"""

    sql = 'SELECT * FROM users WHERE api_key = ?'
    args = [key]
    user = query_db(sql, credentials, args, one=True)
    return user

'''These functions will be for command line database management'''


def open_db(db):
    """open the specified database for our command line management system"""
    try:
        with open(db):
            pass
    except IOError:
        print 'Database does not exist'
        raise NoDatabaseException

    rv = sqlite3.connect(db)
    rv.row_factory = sqlite3.Row

    return rv


def cl_query(query, args):
    """
    command line sql query wrapper
    open the database, execute the specified query and print result if there is one
    """
    try:
        config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')
        db_config = yaml.load(config_stream)
        path = db_config['user_database_configurations'][config_level]['path']

        db = open_db(path)
        cur = db.execute(query, args)
        ret = cur.fetchall()
        db.commit()
        if ret:
            print ret
    except IntegrityError:
        print 'Could not add the same user twice'
    except OperationalError as e:
        print 'Error performing the db operation: {0}'.format(e)
    except NoDatabaseException:
        pass


def add_user(name, idkey=None):
    """add user to database using provided key or new uuid"""

    print 'Users before:'
    get_users()

    if not idkey:
        idkey = str(uuid.uuid4())
    sql = 'INSERT INTO users VALUES(?,?)'
    args = [idkey, name]

    cl_query(sql, args)

    print 'Users after:'
    get_users()


def get_users():
    sql = 'SELECT * FROM users'

    cl_query(sql, [])


def remove_user(idkey):
    """remove user according to key"""
    sql = 'DELETE FROM users WHERE api_key = ?'
    args = [idkey]

    print 'Users before:'
    get_users()

    cl_query(sql, args)

    print 'Users after:'
    get_users()


def init_db():
    """create a new db instance"""
    config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')
    db_config = yaml.load(config_stream)
    path = db_config['user_database_configurations'][config_level]['path']

    try:
        rv = sqlite3.connect(path)
        rv.row_factory = sqlite3.Row

        schema = pkg_resources.resource_stream('web', 'database/schema.sql')

        rv.cursor().executescript(schema.read())
        rv.commit()
    except OperationalError:
        print 'Could not open and execute sql script on db'
    except IOError:
        print 'Schema could not be found'


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Add/Remove/Query Users from a Pir user db. Initialize db as well')
    parser.add_argument("--dev", action="store_true", help="use dev database location")
    subparser = parser.add_subparsers(dest='subparsers')

    # these are our subparsers -- each database
    adder = subparser.add_parser('add_user', help='Adds a user to the db')
    adder.add_argument('user', help='Name of user to add')
    adder.add_argument('--key', help='This key will be assigned to the specified user')

    remover = subparser.add_parser('remove_user', help='Removes user if they exist')
    remover.add_argument('key', help='Name of key to remove')

    user_getter = subparser.add_parser('get_users', help='Returns all users in db')

    db_initializer = subparser.add_parser('db_init', help='Initialize database')

    arguments = parser.parse_args()

    if arguments.dev:
        config_level = 'dev_test'

    subparser_name = arguments.subparsers

    if subparser_name == 'get_users':
        get_users()

    if subparser_name == 'add_user':
        key = arguments.key
        user_name = arguments.user
        add_user(user_name, key)

    if subparser_name == 'remove_user':
        user_key = arguments.key
        remove_user(user_key)

    if subparser_name == 'db_init':
        init_db()