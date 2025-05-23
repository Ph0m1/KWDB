#!/usr/bin/env bash

set -euo pipefail

BETA_TAG=${1-}

if [ -z ${BETA_TAG} ]; then
    echo "usage: $0 <beta-tag>"
    exit 1
fi

darwin=".darwin-10.9-amd64"
linux=".linux-amd64"

function check_aws() {
  echo -n "aws binaries"

  local aws_bins=$(aws s3 ls s3://binaries.kwbasedb.com |
                   grep ${BETA_TAG} | awk '{print $NF}')
  for suffix in ${darwin}.tgz ${linux}.tgz; do
    if ! echo ${aws_bins} | grep -q ${suffix}; then
      echo ": kwbase-${BETA_TAG}${suffix} not found"
      exit 1
    fi
  done

  echo ": ok"
}

function verify_tag() {
  if [ "$1" != "${BETA_TAG}" ]; then
    echo ": expected ${BETA_TAG}, but found $1"
    exit 1
  fi
}

function check_linux() {
  echo -n "linux binary"

  curl -s https://binaries.kwbasedb.com/kwbase-${BETA_TAG}${linux}.tgz | tar xz
  local tag=$($(dirname $0)/../build/builder.sh ./kwbase-${BETA_TAG}${linux}/kwbase version |
              grep 'Build Tag:' | awk '{print $NF}' | tr -d '\r')
  rm -fr ./kwbase-${BETA_TAG}${linux}
  verify_tag "${tag}"

  echo ": ok"
}

function check_darwin() {
  echo -n "darwin binary"

  if [ "$(uname)" != "Darwin" ]; then
    echo ": not checked"
    return
  fi

  curl -s https://binaries.kwbasedb.com/kwbase-${BETA_TAG}${darwin}.tgz | tar xz
  local tag=$(./kwbase-${BETA_TAG}${darwin}/kwbase version |
              grep 'Build Tag:' | awk '{print $NF}' | tr -d '\r')
  rm -fr kwbase-${BETA_TAG}${darwin}
  verify_tag "${tag}"

  echo ": ok"
}

function check_docker() {
  echo -n "docker"

  trap "rm -f docker.stderr" 0
  local tag=$(docker run --rm kwbasedb/kwbase:${BETA_TAG} version 2>docker.stderr |
              grep 'Build Tag:' | awk '{print $NF}' | tr -d '\r')
  if [ -z "${tag}" ]; then
      echo
      cat docker.stderr
      exit 1
  fi
  verify_tag "${tag}"

  echo ": ok"
}

check_aws
check_linux
check_darwin
check_docker
