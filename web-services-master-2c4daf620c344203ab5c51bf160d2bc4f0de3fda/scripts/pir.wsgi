import logging
import pkg_resources
import web.pir_webservice

# activate conda virtual environment for wsgi script to run in
activate_this = '/network/apps/pir/v0.3.2/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

# Very important that the object is called 'application'
config_file = pkg_resources.resource_stream('web', 'prod_log.cfg')
logging.config.fileConfig(config_file)
application = web.pir_webservice.create_app('production')
