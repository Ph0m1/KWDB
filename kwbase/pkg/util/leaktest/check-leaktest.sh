#!/bin/sh

set -e
path=${GOPATH}/src/gitee.com/kwbasedb/kwbase
# pkgs=$(git grep 'go:generate' | grep add-leaktest.sh | awk -F: '{print $1}' | xargs -n1 dirname)
pkgs=$(grep -rl 'go:generate' --exclude-dir=vendor ${path}| xargs grep add-leaktest.sh| awk -F : '{print $1}'| xargs -n1 dirname| \
awk -F "kwbase\/pkg\/" '{print $2}'| sort)
for pkg in ${pkgs}; do
  if [ -z "$(ls ${pkg}/*_test.go 2>/dev/null)" ]; then
    # skip packages without _test.go files.
    continue
  fi

  awk -F'[ (]' '
/func Test.*testing.T\) {/ {
  test = $2
  next
}

/defer leaktest.AfterTest\(.+\)\(\)/ {
  test = 0
  next
}

{
  if (test) {
    printf "%s: %s: missing defer leaktest.AfterTest\n", FILENAME, test
    test = 0
    code = 1
  }
}

END {
  exit code
}
' ${pkg}/*_test.go
done
