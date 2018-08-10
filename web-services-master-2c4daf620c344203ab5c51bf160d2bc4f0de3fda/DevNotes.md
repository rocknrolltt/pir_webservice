Databases:

PIR/etc/dbconfig.yaml

1. user.db
    This is the PIR web service user database. This is small sqlite database
    that holds the api_keys and users.

Login

This uses flask_login


Test Data
=========

So there are monthly updates of many of the perils:
    * Hail
    * Lightning
    * Wind
    * HailRisk
    * Fire

The current convention is to grab the data from

/nas/vc_project/Prometrix/{Peril}/{YYYY}/{MM}

There is the very static DEM data:

/project_static/p1847/Slope