#!/usr/bin/env bash
export DEBUG=${DEBUG:="TRUE"}
export FC_COOKIE_SECRET=${FC_COOKIE_SECRET:="secret"}
export FC_PORT=${FC_PORT:="8889"}
export FC_PUBLIC_URL=${FC_PUBLIC_URL:="http://localhost:8889"}
export FC_QUERY_FILE_LIST_LIMIT=${FC_QUERY_FILE_LIST_LIMIT:="10000"}
#export MONGODB_AUTH_PASS=${MONGODB_AUTH_PASS:=""}
#export MONGODB_AUTH_USER=${MONGODB_AUTH_USER:=""}
export MONGODB_HOST=${MONGODB_HOST:="localhost"}
export MONGODB_PORT=${MONGODB_PORT:="27017"}
export TOKEN_ALGORITHM=${TOKEN_ALGORITHM:="HS512"}
export TOKEN_KEY=${TOKEN_KEY:="secret"}
export TOKEN_URL=${TOKEN_URL:="http://localhost:8888"}
python -m file_catalog
