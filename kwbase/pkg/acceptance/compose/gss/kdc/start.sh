#!/bin/sh

set -e

# The /keytab directory is volume mounted on both kdc and kwbase. kdc
# can create the keytab with kadmin.local here and it is then useable
# by kwbase.
kadmin.local -q "ktadd -k /keytab/kwdb.keytab postgres/gss_kwbase_1.gss_default@MY.EX"

krb5kdc -n
