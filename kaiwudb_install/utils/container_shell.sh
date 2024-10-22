#!/bin/bash

set -e
APP_ROOT=/kaiwudb

CERTS_ROOT=/kaiwudb/certs
CA_FILE=ca.key
if [ ! -d "$CERTS_ROOT" ];then
  mkdir -p $CERTS_ROOT
fi

if [ ! -f "${CERTS_ROOT}/${CA_FILE}" ];then
  ${APP_ROOT}/bin/kwbase cert create-ca --certs-dir=${CERTS_ROOT} --ca-key=${CERTS_ROOT}/${CA_FILE} $SE_MODE
fi

${APP_ROOT}/bin/kwbase cert create-client root --certs-dir=${CERTS_ROOT} --ca-key=${CERTS_ROOT}/${CA_FILE} $SE_MODE

${APP_ROOT}/bin/kwbase cert create-node $@ 127.0.0.1 localhost 0.0.0.0 --certs-dir=${CERTS_ROOT} --ca-key=${CERTS_ROOT}/${CA_FILE} $SE_MODE
