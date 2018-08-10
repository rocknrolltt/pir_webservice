# -*- coding: utf-8 -*-

from sqlconmanager.connection_manager import Manager, ConnectionLevel, ManagerConnectionException
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from datetime import datetime, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from smtplib import SMTPException
from html import HTML
from sqlsoup import SQLSoupError
import smtplib
import yaml
import pkg_resources
import json
import logging
import logging.handlers
import argparse
import glob
import os
import ConfigParser
import sys

def cache():


    # we first find all 'transactions' log files in given directory and attempt to cache all entries in these
    # files into the database
    try:
        con_mgr = Manager()
        db_config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')

        engine = con_mgr.get_connection(db_config_stream, security_level=ConnectionLevel.UPDATE)

        transaction_files = glob.glob(os.path.join(log_file_path, 'transactions*'))
        # temp files may occur if apache is hung up
        temp_files = glob.glob(os.path.join(log_file_path, '.nfs*'))

        files_to_cache = transaction_files + temp_files
        process_cache_count = 0
        for filename in files_to_cache:
            logging.debug('Filename: {0}'.format(filename))
            file_cache_count = 0
            try:
                with open(filename) as trans_file:
                    for line in trans_file:
                        try:
                            trans = json.loads(line)

                            logging.debug('Trying to cache: {0}'.format(trans))

                            trans_id = trans['trans_id']
                            risk = trans['risk_id']
                            street = trans['street']
                            city = trans['city']
                            state = trans['state']
                            postal = trans['postal_code']
                            lat = trans['latitude']
                            lon = trans['longitude']
                            hazards = trans['hazards']
                            geo = trans['geocode_status']
                            result = trans['result_code']
                            time = trans['trans_time']
                            ip = trans['remote_ip']
                            message = trans['message']
                            return_seconds = trans['return_seconds']

                            engine.pir_transactions.insert(trans_id=trans_id, riskid=risk,
                                                           street=street, city=city, state=state, postal_code=postal,
                                                           latitude=lat, longitude=lon, hazard_list=hazards,
                                                           geocode_status=geo, result_code=result, remote_ip=ip,
                                                           trans_time=time, result_message=message,
                                                           return_seconds=return_seconds)

                            engine.commit()
                            logging.info('Successfully cached transaction {0} with code {1}'.format(trans_id, result))
                            file_cache_count = file_cache_count + 1
                        except IntegrityError:
                            # integrity error is thrown if a transaction has already been cached...we can continue
                            # and attempt to cache next transaction despite the error
                            logging.debug("Cannot cache duplicate transaction....Continuing")
                            engine.rollback()
                            pass
                        except SQLAlchemyError as e:
                            # any other sql alchemy error when trying to cache specific transaction,..we can also
                            # continue with next transaction
                            logging.error('Error: {0}'.format(e))
                            logging.info("Caching error....trying to cache next transaction")
                            engine.rollback()
                            pass
                        except KeyError as e:
                            logging.error('KeyError: {0}'.format(e))
                            logging.error('Trying to cache invalid transaction object....moving on to next transaction')
            except IOError as e:
                # if we can't open a transactions file, move on to the next one
                logging.error('IO Error: {0}'.format(e))
                logging.error('Could not open this log file (%s)...trying next file', filename)
                pass
            except ValueError as e:
                logging.error('ValueError: {0}'.format(e))
                logging.error(
                    'Trying to cache transactions from a temporary file (%s) that does not contain transactions...trying '
                    'next file', filename)

            if file_cache_count > 0:
                logging.info('analyzed %s, cached %s new transactions', filename, file_cache_count)

            process_cache_count += file_cache_count

        logging.info('No more transactions to cache')
        logging.info('analyzed %s files, cached %s new transactions', len(files_to_cache), process_cache_count)
        process_cache_count = 0
    except ManagerConnectionException as e:
        logging.error("Could not connect to database server. Caching failed. {0}".format(e))
    except IOError as e:
        logging.error('Could not cache transactions: %s' % str(e))
    except SQLSoupError as e:
        logging.error('Sql Soup Error. Could not cache transactions %s' % str(e))


def construct_report():
    """
    construct an html report of all requests for the current day
    report includes number of request(all, successful, failed) as well as a table of failed requests
    """
    html = HTML()
    html.h1('Daily Report')
    report = {'status': 'OK', 'html': None}

    current_date = date.today()
    date_string = "\'{0}-{1}-{2}\'".format(current_date.year, current_date.month, current_date.day)

    try:
        con_mgr = Manager()
        db_config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')
        engine = con_mgr.get_connection(db_config_stream, security_level=ConnectionLevel.UPDATE, sql_echo=True)

        # # query db to find number of total transactions for current day
        num_transactions = engine.execute(
            "SELECT COUNT(*) FROM pir_transactions WHERE trans_time::date = {0}".format(date_string)).fetchone()

        # query db to get all failed requests for current day
        failed_requests = engine.execute(
            "SELECT * FROM pir_transactions WHERE trans_time::date = {0} AND result_code <> '200'".format(
                date_string)).fetchall()

        # query db to get all request that returned in over 5 seconds
        long_requests = engine.execute(
            "SELECT * FROM pir_transactions WHERE trans_time::date = {0} AND return_seconds > 5".format(
                date_string)).fetchall()

        pro_test_requests = engine.execute(
            "SELECT COUNT(*) FROM pir_transactions WHERE trans_time::date = {0} AND remote_ip IN "
            "('10.16.153.171', '10.16.153.105')".format(date_string)).fetchone()

        pro_acceptance_requests = engine.execute(
            "SELECT COUNT(*) FROM pir_transactions WHERE trans_time::date = {0} AND remote_ip IN "
            "('10.20.201.2', '10.20.201.32')".format(date_string)).fetchone()

        pro_production_requests = engine.execute(
            "SELECT COUNT(*) FROM pir_transactions WHERE trans_time::date = {0} AND remote_ip LIKE '10.65.1%'".format(
                date_string)).fetchone()

        pro_dev_requests = engine.execute(
            "SELECT COUNT(*) FROM pir_transactions WHERE trans_time::date = {0} AND remote_ip LIKE '172.16.132%'".format(
                date_string)).fetchone()

        # construct summary table
        html.h2('Summary')
        summary_table = html.table(cellpadding='10')
        num_transactions_row = summary_table.tr
        num_transactions_row.td("Number of Requests")
        num_transactions_row.td(str(num_transactions[0]))
        success_row = summary_table.tr
        success_row.td("Number of Successful Requests")
        success_row.td(str(num_transactions[0] - len(failed_requests)))
        failure_row = summary_table.tr
        failure_row.td("Number of Failed Requests")
        failure_row.td(str(len(failed_requests)))
        long_row = summary_table.tr
        long_row.td("Number of Requests Over 5 Seconds")
        long_row.td(str(len(long_requests)))

        # construct request origin table -- this is based off of given ip addresses in shared confluence page
        html.h2('Prometrix Request Origin')
        origin_table = html.table(cellpadding='10')
        pro_dev_row = origin_table.tr
        pro_dev_row.td("Dev")
        pro_dev_row.td(str(pro_dev_requests[0]))
        pro_test_row = origin_table.tr
        pro_test_row.td("Test")
        pro_test_row.td(str(pro_test_requests[0]))
        pro_acceptance_row = origin_table.tr
        pro_acceptance_row.td("Acceptance")
        pro_acceptance_row.td(str(pro_acceptance_requests[0]))
        pro_production_row = origin_table.tr
        pro_production_row.td("Production")
        pro_production_row.td(str(pro_production_requests[0]))

        if long_requests:
            html.h2("Requests Over 5 Seconds")

            long_table = html.table(border="1")

            header_row = long_table.tr
            header_row.th('Transaction ID')
            header_row.th('Geocode Status')
            header_row.th('Address')
            header_row.th('HTTP Result Code')
            header_row.th('HTTP Result Message')
            header_row.th('IP')
            header_row.th('Transaction Date')
            header_row.th('Seconds to Return')

            for trans in long_requests:
                address = trans['street'] + ', ' + trans['city'] + ', ' + trans['state'] + ' ' + trans['postal_code']

                row = long_table.tr
                row.td(trans['trans_id'])
                row.td(str(trans['geocode_status']))
                row.td(address)
                row.td(trans['result_code'])
                row.td(trans['result_message'])
                row.td(trans['remote_ip'])
                row.td(str(trans['trans_time']))
                row.td(str(trans['return_seconds']))

            if len(long_requests) > monitor_config['monitoring_configurations'][config_level]['long_threshold']:
                report['status'] = 'ERROR'
            else:
                report['status'] = "WARNING"

        # if we have failed requests, include them in report
        if failed_requests:
            html.h2('Failed Requests')

            failed_table = html.table(border="1")

            header_row = failed_table.tr
            header_row.th('Transaction ID')
            header_row.th('Geocode Status')
            header_row.th('Address')
            header_row.th('HTTP Result Code')
            header_row.th('HTTP Result Message')
            header_row.th('IP')
            header_row.th('Transaction Date')
            header_row.th('Seconds To Return')

            for trans in failed_requests:
                address_components = [trans['street'], trans['city'], trans['state'], trans['postal_code']]

                address = ', '.join(filter(None, address_components))

                row = failed_table.tr
                row.td(trans['trans_id'])
                row.td(str(trans['geocode_status']))
                row.td(address)
                row.td(trans['result_code'])
                row.td(trans['result_message'])
                row.td(trans['remote_ip'])
                row.td(str(trans['trans_time']))
                row.td(str(trans['return_seconds']))

            # if there are enough failed requests, indicate an error
            # if there is a failed request, indicate a warning
            if len(failed_requests) > monitor_config['monitoring_configurations'][config_level]['failed_threshold']:
                report['status'] = 'ERROR'

            if report['status'] != 'ERROR':
                report['status'] = 'WARNING'

    except ManagerConnectionException as e:
        logging.error("Could not connect to database server. Caching failed. {0}".format(e))
        html.p('COULD NOT CONNECT TO DATABASE')
        report['status'] = 'ERROR'
    except SQLAlchemyError as e:
        logging.error('Error: {0}'.format(e))
        logging.error('Unable to retrieve desired information from database')
        html.p('SQL ERROR: {0}'.format(e))
        report['status'] = 'ERROR'

    report['html'] = str(html)

    return report


def send_report():
    """
    send monitoring report to specified email address
    """

    server = monitor_config['monitoring_configurations'][config_level]['server']
    e_from = monitor_config['monitoring_configurations'][config_level]['from']
    e_to = monitor_config['monitoring_configurations'][config_level]['to']
    today = datetime.today().strftime('%Y-%m-%d')

    report = construct_report()
    status = report['status']
    html = report['html']

    output = MIMEText(html, 'html')

    msg = MIMEMultipart()
    msg['Subject'] = '[{0}] PIR Status Report {1}'.format(status, today)
    msg['From'] = e_from
    msg['To'] = ", ".join(e_to)
    msg.attach(output)

    # attempt to send report to specified email address
    # if report cannot be sent, a SMTP exception will be thrown/caught
    try:
        smtp_server = smtplib.SMTP(server)
        smtp_server.sendmail(e_from, e_to, msg.as_string())
        smtp_server.quit()
    except SMTPException as e:
        logging.error("Email could not be sent: {0}".format(e))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--report', action='store_true', help="Email current day transaction report")
    parser.add_argument('--cache', action='store_true', help='Cache transactions')
    parser.add_argument('--dev', action='store_true',
                        help="Dev flag indicates if we send email to developer for development purposes")
    parser.add_argument('--verbose', '-v', action='store_true', help="verbose logging")

    args = parser.parse_args()

    log_level = logging.INFO
    if args.verbose is True:
        log_level = logging.DEBUG

    my_logger = logging.getLogger()
    my_logger.setLevel(log_level)

    cp = ConfigParser.ConfigParser()

    # if production, we need to activate the python environment the application is running in
    # the log file directory is located in /network/apps/pir/{environment}/log
    if args.dev:
        log_config_file = pkg_resources.resource_filename('web', 'dev_log.cfg')
    else:
        log_config_file = pkg_resources.resource_filename('web', 'prod_log.cfg')

    cp.read(log_config_file)

    # trim the open and closed parentheses and quoting... this seems kinda brittle
    log_file_path = os.path.dirname(cp.get('handler_file', 'args')[2:-3])
    cache_log_file_path = os.path.join(log_file_path, 'caching_temp.log')
    handler = logging.handlers.RotatingFileHandler(cache_log_file_path, maxBytes=50000000, backupCount=3)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s][p%(process)d][%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    my_logger.addHandler(handler)

    config_level = 'production'
    if args.dev:
        config_level = 'dev_test'

    config_stream = pkg_resources.resource_stream('PIR', 'etc/dbconfig.yaml')
    monitor_config = yaml.load(config_stream)

    if args.cache:
        cache()

    if args.report:
        send_report()

