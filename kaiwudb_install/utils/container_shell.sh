#!/bin/bash

set -e
APP_ROOT=/kaiwudb

CERTS_ROOT=/kaiwudb/certs
CA_FILE=ca.key
if [ ! -d "$CERTS_ROOT" ];then
  mkdir -p $CERTS_ROOT
fi

if [ ! -f "${CERTS_ROOT}/${CA_FILE}" ];then
  ${APP_ROOT}/bin/kwbase cert create-ca --certs-dir=${CERTS_ROOT} --ca-key=${CERTS_ROOT}/${CA_FILE}
fi

${APP_ROOT}/bin/kwbase cert create-client root --certs-dir=${CERTS_ROOT} --ca-key=${CERTS_ROOT}/${CA_FILE}
openssl pkcs8 -topk8 -inform PEM -outform DER -in ${CERTS_ROOT}/client.root.key -out ${CERTS_ROOT}/client.root.pk8 -nocrypt

${APP_ROOT}/bin/kwbase cert create-node $@ 127.0.0.1 localhost 0.0.0.0 --certs-dir=${CERTS_ROOT} --ca-key=${CERTS_ROOT}/${CA_FILE}
