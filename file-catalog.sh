#!/usr/bin/env bash
export AUTH_AUDIENCE=${AUTH_AUDIENCE:="file-catalog"}
export AUTH_OPENID_URL=${AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export DEBUG=${DEBUG:="TRUE"}
export FC_COOKIE_SECRET=${FC_COOKIE_SECRET:="736563726574"}  # "secret" in hex
export FC_PORT=${FC_PORT:="8889"}
export FC_PUBLIC_URL=${FC_PUBLIC_URL:="http://localhost:8889"}
export FC_QUERY_FILE_LIST_LIMIT=${FC_QUERY_FILE_LIST_LIMIT:="10000"}
# export MONGODB_AUTH_PASS=${MONGODB_AUTH_PASS:=""}
# export MONGODB_AUTH_SOURCE_DB=${MONGODB_AUTH_SOURCE_DB:="admin"}
# export MONGODB_AUTH_USER=${MONGODB_AUTH_USER:=""}
export MONGODB_HOST=${MONGODB_HOST:="localhost"}
export MONGODB_PORT=${MONGODB_PORT:="27017"}
# export MONGODB_URI=${MONGODB_URI:=""}
python -m file_catalog
