// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package install

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/util/httputil"
)

const (
	edgeBinaryServer    = "https://edge-binaries.kwbasedb.com"
	releaseBinaryServer = "https://s3.amazonaws.com/binaries.kwbasedb.com/"
)

func getEdgeURL(urlPathBase, SHA, arch string, ext string) (*url.URL, error) {
	edgeBinaryLocation, err := url.Parse(edgeBinaryServer)
	if err != nil {
		return nil, err
	}
	edgeBinaryLocation.Path = urlPathBase
	// If a target architecture is provided, attach that.
	if len(arch) > 0 {
		edgeBinaryLocation.Path += "." + arch
	}
	// If a specific SHA is provided, just attach that.
	if len(SHA) > 0 {
		edgeBinaryLocation.Path += "." + SHA + ext
	} else {
		edgeBinaryLocation.Path += ext + ".LATEST"
		// Otherwise, find the latest SHA binary available. This works because
		// "[executable].LATEST" redirects to the latest SHA.
		resp, err := httputil.Head(context.TODO(), edgeBinaryLocation.String())
		if err != nil {
			return nil, err
		}
		edgeBinaryLocation = resp.Request.URL
	}

	return edgeBinaryLocation, nil
}

// StageRemoteBinary downloads a kwbase edge binary with the provided
// application path to each specified by the cluster. If no SHA is specified,
// the latest build of the binary is used instead.
// Returns the SHA of the resolve binary.
func StageRemoteBinary(
	c *SyncedCluster, applicationName, urlPathBase, SHA, arch string,
) (string, error) {
	binURL, err := getEdgeURL(urlPathBase, SHA, arch, "")
	if err != nil {
		return "", err
	}
	fmt.Printf("Resolved binary url for %s: %s\n", applicationName, binURL)
	urlSplit := strings.Split(binURL.Path, ".")
	cmdStr := fmt.Sprintf(
		`curl -sfSL -o %s "%s" && chmod 755 ./%s`, applicationName, binURL, applicationName,
	)
	return urlSplit[len(urlSplit)-1], c.Run(
		os.Stdout, os.Stderr, c.Nodes, fmt.Sprintf("staging binary (%s)", applicationName), cmdStr,
	)
}

// StageOptionalRemoteLibrary downloads a library from the kwbase edge with the provided
// application path to each specified by the cluster.
// If no SHA is specified, the latest build of the library is used instead.
// It will not error if the library does not exist on the edge.
func StageOptionalRemoteLibrary(
	c *SyncedCluster, libraryName, urlPathBase, SHA, arch, ext string,
) error {
	url, err := getEdgeURL(urlPathBase, SHA, arch, ext)
	if err != nil {
		return err
	}
	fmt.Printf("Resolved library url for %s: %s\n", libraryName, url)
	cmdStr := fmt.Sprintf(
		`mkdir -p ./lib && \
curl -sfSL -o "./lib/%s" "%s" 2>/dev/null || echo 'optional library %s not found; continuing...'`,
		libraryName+ext,
		url,
		libraryName+ext,
	)
	return c.Run(
		os.Stdout, os.Stderr, c.Nodes, fmt.Sprintf("staging library (%s)", libraryName), cmdStr,
	)
}

// StageCockroachRelease downloads an official CockroachDB release binary with
// the specified version.
func StageCockroachRelease(c *SyncedCluster, version, arch string) error {
	if len(version) == 0 {
		return fmt.Errorf(
			"release application cannot be staged without specifying a specific version",
		)
	}
	binURL, err := url.Parse(releaseBinaryServer)
	if err != nil {
		return err
	}
	binURL.Path += fmt.Sprintf("kwbase-%s.%s.tgz", version, arch)
	fmt.Printf("Resolved release url for kwbase version %s: %s\n", version, binURL)

	// This command incantation:
	// - Creates a temporary directory on the remote machine
	// - Downloads and unpacks the kwbase release into the temp directory
	// - Moves the kwbase executable from the binary to '/.' and gives it
	// the correct permissions.
	cmdStr := fmt.Sprintf(`
tmpdir="$(mktemp -d /tmp/kwbase-release.XXX)" && \
curl -f -s -S -o- %s | tar xfz - -C "${tmpdir}" --strip-components 1 && \
mv ${tmpdir}/kwbase ./kwbase && \
mkdir -p ./lib && \
if [ -d ${tmpdir}/lib ]; then mv ${tmpdir}/lib/* ./lib; fi && \
chmod 755 ./kwbase
`, binURL)
	return c.Run(
		os.Stdout, os.Stderr, c.Nodes, "staging kwbase release binary", cmdStr,
	)
}
