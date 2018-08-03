#!/bin/sh

virtualenv --clear --no-site-packages env
echo "unset PYTHONPATH" >> env/bin/activate
. env/bin/activate
pip install pymongo ldap3 pyasn1 tornado PyJWT futures
echo "'source env/bin/activate' to use virtual env"
