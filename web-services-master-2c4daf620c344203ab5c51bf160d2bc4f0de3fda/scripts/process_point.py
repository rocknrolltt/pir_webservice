# -*- coding: utf-8 -*-
"""
Created on Tue Feb  3 13:21:34 2015

@author: bwoods
"""
from datetime import datetime
import logging
import yaml

from PIR.load_point import process_point
import PIR.output_xml as oxml

logger = logging.getLogger("PIR")
LOGFORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=LOGFORMAT)
logger.addHandler(logging.NullHandler())


if __name__ == '__main__':
    from argparse import ArgumentParser, RawDescriptionHelpFormatter

    parser = ArgumentParser(description="Find the coordinates of a pint",
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('cfgfile', type=str,
                        help='Path to config file')
    parser.add_argument('-t', '--transaction', type=str,
                        help='Transaction ID to include in XML. Defaults'
                             'to the current time string',
                        default=unicode(datetime.utcnow()))
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

    # unpack required information from config file
    with open(args.cfgfile, 'r') as f:
        cfg = yaml.load(f)

    # run two test reports

    tic = datetime.utcnow()
    location = oxml.Location('Sequoia Cider Mill', '40311 Sierra Dr', '',
                             'Three Rivers', 'CA', '93271',
                             lat=36.418343, lon=-118.922271)

    result = process_point(location, args.transaction, cfg)
    print "result: ", result
    print "time ellapsed %s" % (datetime.utcnow()-tic)
    with open('Sequoia.xml', 'w') as f:
        f.write(unicode(result))

    tic = datetime.utcnow()
    location = oxml.Location('Some business', '474 Central Ave', '',
                             'Highland Park', 'IL', '60035-2680',
                             lat=42.186353, lon=-87.797327)

    result = process_point(location, args.transaction, cfg)
    print "result:", result
    print "time ellapsed %s" % (datetime.utcnow()-tic)
    with open('Illinois.xml', 'w') as f:
        f.write(unicode(result))
