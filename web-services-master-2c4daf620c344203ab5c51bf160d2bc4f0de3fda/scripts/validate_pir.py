#!/bin/env python

import logging
import glob
import json
import re
import sys
import os
import os.path
import difflib

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from lxml import etree
import xtdiff

import datadiff

CACHE = None

def get_endpoints(endpoint_strings):
    '''Parses endpoint strings with keys and returns a list of { url: ..., key: ...} dictionaries.'''

    endpoints = []
    endpoint_errors = []
    split_password_re = re.compile(r'^(.*)<(\S+)>$')
    for end_str in endpoint_strings:
        match = split_password_re.match(end_str)
        if match is not None:
            cache_id = match.group(1)
            cache_id = re.sub(r'https{0,1}://', '', cache_id)
            cache_id = re.sub(r'/$', '', cache_id)
            cache_id = re.sub('[^\w^\.\s-]', '_', cache_id).strip().lower()
            endpoint = { 'url' : match.group(1), 'key' : match.group(2), 'cache_id' : cache_id }
            endpoints.append(endpoint)
        else:
            logging.error("can't parse %s", end_str)
            endpoint_errors.append(end_str)
    if len(endpoint_errors) > 0:
        raise RuntimeError("Can't parse %s"%(end_str))

    return endpoints

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

def validate_endpoints(endpoints, skip_validation=False):
    '''Validate that this is indeed a reachable PIR API endpoint.'''

    endpoint_errors = []
    for endpoint in endpoints:
        url = '{}/report/'.format(endpoint['url'])
        if skip_validation:
            logging.info("skip testing url %s", url)
            continue
        logging.debug("testing url %s", url)

        request_body = get_valid_request()
        request_body["key"] = endpoint["key"]
        logging.debug("request_body %s", request_body)

        try:
            r = requests.post(url, json=request_body, verify=False)
        except requests.exceptions.ConnectionError as ex:
            logging.error(ex)
            endpoint_errors.append(url)
        else:
            if r.status_code != 200:
                logging.error("could not get a valid response from %s", url)
                logging.debug(r.status_code)
                logging.debug(r.text)

                endpoint_errors.append(url)
            else:
                logging.debug(r.text)

    return endpoint_errors

def get_cache(cache_key):
    '''gets a reference to the cache for reading or writing.'''
    cache_base = "cache"

    contents = None
    try:
        if not os.path.isdir(os.path.join(cache_base, cache_key[0])):
            logging.debug("make directory... %s", os.path.join(cache_base, cache_key[0]))
            os.makedirs(os.path.join(cache_base, cache_key[0]))

        cache_file = os.path.join(cache_base, cache_key[0], cache_key[1])
        if os.path.isfile(cache_file):
            with open(cache_file, 'r') as cf:
                contents = "\n".join(cf.readlines())
    except BaseException as ex:
        logging.exception(ex)

    return contents

def put_cache(cache_key, contents):
    '''Put a entry into the cache.'''
    cache_base = "cache"

    try:
        if not os.path.isdir(os.path.join(cache_base, cache_key[0])):
            logging.debug("make directory... %s", os.path.join(cache_base, cache_key[0]))
            os.makedirs(os.path.join(cache_base, cache_key[0]))

        cache_file = os.path.join(cache_base, cache_key[0], cache_key[1])

        with open(cache_file, 'w') as cf:
            logging.debug("opened cache file : %s", cache_file)
            cf.write(contents)
    except BaseException as ex:
        logging.error("Error writing to cache...")
        logging.exception(ex)

    return True

def get_pir_report(endpoint, test_info, use_cache=True):
    '''Get the PIR Report from the endpoint.'''

    cache_key = (endpoint['cache_id'], test_info['test_id'])

    request = test_info['request']
    request["key"] = endpoint["key"]

    contents = None
    from_network = False
    # try to get from cache...
    if use_cache:
        contents = get_cache(cache_key)
    logging.debug("contents %s", contents)

    # try to get from network resource...
    if contents is None:
        logging.debug("not available in cache, fetch from network.")
        try:
            logging.debug("trying url: %s", endpoint["url"])
            logging.debug("payload: %s", request)
            r = requests.post("{endpoint}/report/".format(endpoint=endpoint["url"]), json=request, verify=False)
            contents = r.text
            logging.debug("HTTP CODE: %s", r.status_code)
            logging.debug("contents: %s", contents)
            from_network = True
        except requests.exceptions.ConnectionError as ex:
            logging.error(ex)
            return None

    if use_cache and contents is not None and from_network:
        put_cache(cache_key, contents)

    return contents

def diff_pir_report(baseline, update):
    '''Get a diff of the two XML strings (baseline, update)'''
    logging.debug("diffing... BASELINE %s", baseline)
    logging.debug("===============================================")
    logging.debug("diffing... UPDATE   %s", update)
    left = etree.fromstring(baseline)
    logging.debug("parsed baseline")
    right = etree.fromstring(update)
    logging.debug("parsed update")
    left_pretty_lines = etree.tostring(left, pretty_print=True).split("\n")
    right_pretty_lines = etree.tostring(right, pretty_print=True).split("\n")

    d = difflib.HtmlDiff()
    diff = d.make_table(left_pretty_lines, right_pretty_lines, context=True)


    return diff

def generate_test(test_id, street, city, state, zip, lat, lon):
    template = { "property_id" : test_id,
                 "key" : None,
                 "address" : {"street" : street,
                              "city" : city,
                              "state" : state,
                              "postal_code" : zip,
                              "country" : "USA"},
                 "coordinates" : { "latitude" : lat, "longitude" : lon },
                 "hazard_list" : [ "lightning", "hail", "fire", "wind" ]
               }

    pir_test = {}
    pir_test['request'] = template
    pir_test['description'] = '{} - {}, {}, {} {} ({}, {})'.format(test_id, street, city, state, zip, lat, lon)
    pir_test['test_id'] = test_id

    return pir_test


if __name__ == "__main__":
    '''Valdate PIR by comparing one PIR instance to a reference file or to another instance.'''
    import argparse

    parser = argparse.ArgumentParser(description='Validate PIR instance')
    parser.add_argument('test_file', help='A test description.')
    parser.add_argument('endpoints', metavar='endpoint', nargs='+', help='End points (format: https://path/to/endpoint/<API_KEY>)')
    parser.add_argument('-v', '--verbose', dest='verbose', help='Increase logging levels.', action='count')
    parser.add_argument('-s', '--skip_validation', dest='skip_validation', action='store_true', help='Skip endpoint validation')
    parser.add_argument('-c', '--use_cache', dest='use_cache', action='store_true', help='Use cache')

    args = parser.parse_args()
    log_level = logging.WARN
    if args.verbose > 1:
        log_level = logging.DEBUG
    elif args.verbose > 0:
        log_level = logging.INFO

    # set up logging
    logging.basicConfig(level=log_level)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("datadiff").setLevel(logging.WARNING)

    endpoints = get_endpoints(args.endpoints)
    endpoint_errors = validate_endpoints(endpoints, args.skip_validation)
    if endpoint_errors:
        logging.info("there was an error connecting to %d of %d endpoints",
                     len(endpoint_errors), len(endpoints))
        for endpoint in endpoint_errors:
            logging.info("    url: %s", endpoint)
        sys.exit(-1)


    # parse test addresses file
    address_tests = []
    print "<pre>"
    with open(args.test_file, 'r') as address_file:
        for line in address_file:
            line = line.rstrip()
            (test_id, street, city, state, zip, lat, lon) = line.split(",")
            print "<a href {0:25s} {1:20s} {2:2s} {3:5s}".format(street, city, state, zip)
            req_dict = generate_test(test_id, street, city, state, zip, lat, lon)
            address_tests.append(req_dict)
    print "</pre>"

    differences = {}
    for a_test in address_tests:
        diffs = []

        baseline_response = None
        for endpoint in endpoints:
            try:
                pir_resp = get_pir_report(endpoint, a_test)
                if baseline_response is None:
                    baseline_response = pir_resp
                else:
                    logging.warn("GET RESPONSE DIFFS not implemented...")
                    diff = diff_pir_report(baseline_response, pir_resp)
                    if diff is not None:
                        logging.info("location %s", a_test['description'])
                        logging.info(diff)
                        diffs.append(diff)
            except KeyError as ex:
                logging.error(a_test)
                logging.exception(ex)
        print "<h1><a id='{test_id}'>{description}</a></h1>".format(**a_test)
        print "\n".join(diffs)
        differences[a_test['description']] = diffs

#     for test in differences:
#         print "<h1><a id={}</h1>".format(test)
#         print "\n".join(differences[test])
