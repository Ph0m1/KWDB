# RPM and DEB package manual
## RPM packaging steps
Enter the directory:
```cd $GOPATH/src/gitee.com/kwbasedb/packages/rpmpackage/SPECS```
Execute the packaging commands:
```rpmbuild --define "_topdir %(echo $GOPATH)/src/gitee.com/kwbasedb/packages/rpmpackage" -v -D "kaiwudb 1.2.1" -ba kaiwudb.spec```

## DEB packaging steps
Enter the directory:
```cd $GOPATH/src/github.com/ZDP/packages/debpackage```
- If using GPG to sign source code packages and change files
    1. Generate GPG key:
    ```gpg --full-generate-key```
    2. Execute the packaging commands:
    ```dpkg-buildpackage```
- If not using GPG to sign source code packages and change files
    1. Execute the packaging commands:
    ```dpkg-buildpackage -us -uc```

# List of DEB and RPM packaged files
1).server
install/bin/kwbase
install/lib/libkwdbts2.so

2).libcommon
install/lib/libcommon.so
