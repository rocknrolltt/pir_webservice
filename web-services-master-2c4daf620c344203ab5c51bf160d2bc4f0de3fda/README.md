PIR Web Service
===============


INSTALL
-------

$ python setup.py install

edit the configuration files:

    PIR/etc/dbconfig.yaml
        this contains information about the system:
            * database_configurations
            * monitoring_configurations
            * user_database_configurations

        each of these sections have different "tags" for different "environments"

    PIR/etc/datasets.cfg
        this contains information about the currency and the paths of the data

DEPLOY
------
A data set is configured via the PIR/etc/datasets.cfg configuration file.
