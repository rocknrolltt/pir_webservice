#!/usr/bin/env python

from setuptools import setup

# dynamically detect the version from __init__.py
import os.path

for line in open(os.path.join(os.path.dirname(__file__), 'PIR',
                              '__init__.py')).readlines():
    if line.startswith('__version__'):
        exec (line.strip())
        break
else:
    raise IOError("Missing __version__ definition in __init__.py")

INSTALL_REQUIRES = [
    "requests",
    "Flask",
    "flask-restful",
    "Flask-Login",
    "geocode",
    "pyyaml",
    "sqlalchemy",
    "gdal",
    "pandas",
    "xmltodict",
    "xlrd",
    "sqlconmanager",
    "blinker",
    "html",
    "datadiff",
    "geocode"
]

setup(name='PIR',
      version=__version__,
      author='Bryan Woods',
      author_email='bwoods@aer.com',
      description='A package containing multiple modules for reading'
                  ' peril data and delivering PIR XML files to'
                  ' Prometrix via a RESTful API.',
      packages=['PIR', 'web/database', 'web'],
      scripts=['web/pir_webservice.py', 'scripts/process_point.py',
               'scripts/pir.wsgi', 'scripts/cache_transactions.py', 'web/database/manage_db.py'],
      # Bypass zipping on build/install process to avoid
      # zip-cache bugs on NFS filesystems.
      zip_safe=False,
      install_requires=INSTALL_REQUIRES,
      setup_requires=['nose>=1.0', 'flask'],
      test_suite='nose.collector',
      package_data={"web": ['dev_log.cfg', 'prod_log.cfg', 'example.xml', 'database/schema.sql'],
                    'PIR': ['tests/TestAddresses.xlsx', 'tests/Illinois.xml',
                            'etc/datasets.cfg', 'etc/dbconfig.yaml']}
)
