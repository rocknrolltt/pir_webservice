# -*- coding: utf-8 -*-
"""
Created on Tue Feb  3 13:21:34 2015

@author: bwoods
"""

import logging
from sqlalchemy import create_engine
from pandas.io.parsers import read_csv

logger = logging.getLogger("PIR")
LOGFORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=LOGFORMAT)
logger.addHandler(logging.NullHandler())

if __name__ == '__main__':
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser(description="Load the Prometrix commercial "
                                        "property address database.",
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('file_path', type=str, help='Path to the file')
    parser.add_argument('--table_name', type=str,
                        default='prometrix_addresses',
                        help='Name of SQL table. Default: %(default)s')
    parser.add_argument('--chunksize', type=int, default=0,
                        help="Number of entries to commit at one time. "
                        "Default: %(default)s")
    parser.add_argument("--log", dest='logfile', type=str,
                        help="Log file. If not given, then log messages to"
                             " the screen using stderr.",
                        default=None)
    parser.add_argument("--verbose", "-v", action='count',
                        help='Increase logging verbosity (repeat for more'
                             ' verbose logs)', default=0)

    args = parser.parse_args()

    loglevs = {0: logging.ERROR,
               1: logging.WARNING,
               2: logging.INFO,
               3: logging.DEBUG}
    if args.verbose > 3:
        args.verbose = 3

    log_level = loglevs[args.verbose]

    if args.logfile is not None:
        logging.basicConfig(filename=args.logfile, filemode='a',
                            format="%(message)s", level=log_level)
    else:
        logging.basicConfig(level=log_level)

    table = args.table_name

    df = read_csv(args.file_path, error_bad_lines=False)
    # break the dataframe into chunks
    chunksize = args.chunksize
    engine = create_engine('postgresql://bwoods:bwoods@lex-pgdb01:5432/'
                           'damage_test')
    # create the table
    engine.execute('DROP TABLE IF EXISTS %s' % table)
    engine.execute('''CREATE TABLE %s (
     riskid         VARCHAR(12) PRIMARY KEY,
     lownumber      text,
     highnumber     text,
     predirection   text,
     streetname     text,
     streettype     text,
     postdirection  text,
     city           text,
     postalcity     text,
     statecode      VARCHAR(2),
     zip            NUMERIC(5),
     zip4           NUMERIC(4),
     lat            REAL,
     long           REAL
   )''' % table)

    if chunksize:
        nrows = len(df)
        chunks = int(nrows / chunksize) + 1
        # connect to the database
        for i in range(chunks):
            start_i = i * chunksize
            end_i = min((i + 1) * chunksize, nrows)
            if start_i >= end_i:
                break
            logger.debug("processing %i to %i", start_i, end_i)
            snippet = df[start_i:end_i]
            logger.debug('snippet %s', snippet)
            snippet.to_sql(table, engine, index=False, if_exists='append')
    else:
        df.to_sql(table, engine, index=False, if_exists='append')
