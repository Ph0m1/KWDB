// Copyright 2019 The Cockroach Authors.
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

package main

import (
	"context"
	"fmt"
	"regexp"
)

var djangoReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)(\.(?P<point>\d+))?$`)
var djangoCockroachDBReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)$`)

var djangoSupportedTag = "kwbase-3.1.x"

func registerDjango(r *testRegistry) {
	runDjango := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up kwbase")
		c.Put(ctx, kwbase, "./kwbase", c.All())
		c.Start(ctx, t, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		err = alterZoneConfigAndClusterSettings(ctx, version, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		t.Status("cloning django and installing prerequisites")

		if err := repeatRunE(
			ctx, c, node, "update apt-get",
			`
				sudo add-apt-repository ppa:deadsnakes/ppa &&
				sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install make python3.7 libpq-dev python3.7-dev gcc python3-setuptools python-setuptools build-essential`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "set python3.7 as default", `
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.5 1
    		sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
    		sudo update-alternatives --config python3`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install pip",
			`curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.7`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install pytest",
			`sudo pip3 install pytest pytest-xdist psycopg2`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old django", `rm -rf /mnt/data1/django`,
		); err != nil {
			t.Fatal(err)
		}

		djangoLatestTag, err := repeatGetLatestTag(
			ctx, c, "django", "django", djangoReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest Django release is %s.", djangoLatestTag)
		c.l.Printf("Supported Django release is %s.", djangoSupportedTag)

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/timgraham/django/",
			"/mnt/data1/django",
			djangoSupportedTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		djangoCockroachDBLatestTag, err := repeatGetLatestTag(
			ctx, c, "kwbasedb", "django-kwbasedb", djangoCockroachDBReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest django-kwbasedb release is %s.", djangoCockroachDBLatestTag)

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/kwbasedb/django-kwbasedb",
			"/mnt/data1/django/tests/django-kwbasedb",
			"master",
			node,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install django's dependencies", `
				cd /mnt/data1/django/tests &&
				sudo pip3 install -e .. &&
				sudo pip3 install -r requirements/py3.txt &&
				sudo pip3 install -r requirements/postgres.txt`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install django-kwbasedb", `
					cd /mnt/data1/django/tests/django-kwbasedb/ &&
					sudo pip3 install .`,
		); err != nil {
			t.Fatal(err)
		}

		// Write the kwbase config into the test suite to use.
		if err := repeatRunE(
			ctx, c, node, "configuring tests to use kwbase",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/django/tests/kwbase_settings.py",
				kwbaseDjangoSettings,
			),
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailureList, ignoredlistName, ignoredlist := djangoBlocklists.getLists(version)
		if expectedFailureList == nil {
			t.Fatalf("No django blocklist defined for kwbase version %s", version)
		}
		if ignoredlist == nil {
			t.Fatalf("No django ignorelist defined for kwbase version %s", version)
		}
		c.l.Printf("Running kwbase version %s, using blocklist %s, using ignoredlist %s",
			version, blocklistName, ignoredlistName)

		// TODO (rohany): move this to a file backed buffer if the output becomes
		//  too large.
		var fullTestResults []byte
		for _, testName := range enabledDjangoTests {
			t.Status("Running django test app ", testName)
			// Running the test suite is expected to error out, so swallow the error.
			rawResults, _ := c.RunWithBuffer(
				ctx, t.l, node, fmt.Sprintf(djangoRunTestCmd, testName))
			fullTestResults = append(fullTestResults, rawResults...)
			c.l.Printf("Test results for app %s: %s", testName, rawResults)
			c.l.Printf("Test stdout for app %s:", testName)
			if err := c.RunL(
				ctx, t.l, node, fmt.Sprintf("cd /mnt/data1/django/tests && cat %s.stdout", testName),
			); err != nil {
				t.Fatal(err)
			}
		}
		t.Status("collating test results")

		results := newORMTestsResults()
		results.parsePythonUnitTestOutput(fullTestResults, expectedFailureList, ignoredlist)
		results.summarizeAll(
			t, "django" /* ormName */, blocklistName, expectedFailureList, version, djangoSupportedTag,
		)
	}

	r.Add(testSpec{
		MinVersion: "v20.2.0",
		Name:       "django",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1, cpu(16)),
		Tags:       []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runDjango(ctx, t, c)
		},
	})
}

// Test results are only in stderr, so stdout is redirected and printed later.
const djangoRunTestCmd = `
cd /mnt/data1/django/tests &&
RUNNING_KWBASE_BACKEND_TESTS=1 python3 runtests.py %[1]s --settings kwbase_settings --parallel 1 -v 2 > %[1]s.stdout
`

const kwbaseDjangoSettings = `
from django.test.runner import DiscoverRunner


DATABASES = {
    'default': {
        'ENGINE': 'django_kwbasedb',
        'NAME': 'django_tests',
        'USER': 'root',
        'PASSWORD': '',
        'HOST': 'localhost',
        'PORT': 26257,
    },
    'other': {
        'ENGINE': 'django_kwbasedb',
        'NAME': 'django_tests2',
        'USER': 'root',
        'PASSWORD': '',
        'HOST': 'localhost',
        'PORT': 26257,
    },
}
SECRET_KEY = 'django_tests_secret_key'
PASSWORD_HASHERS = [
    'django.contrib.auth.hashers.MD5PasswordHasher',
]
TEST_RUNNER = '.kwbase_settings.NonDescribingDiscoverRunner'

class NonDescribingDiscoverRunner(DiscoverRunner):
    def get_test_runner_kwargs(self):
        return {
            'failfast': self.failfast,
            'resultclass': self.get_resultclass(),
            'verbosity': self.verbosity,
            'descriptions': False,
        }
`
