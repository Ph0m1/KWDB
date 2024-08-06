// Copyright 2015 The Cockroach Authors.
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

package build

import (
	"fmt"
	"runtime"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/version"
)

// TimeFormat is the reference format for build.Time. Make sure it stays in sync
// with the string passed to the linker in the root Makefile.
const TimeFormat = "2006/01/02 15:04:05"

var (
	// These variables are initialized via the linker -X flag in the
	// top-level Makefile when compiling release binaries.
	tag             = "unknown" // Tag of this build (git describe --tags w/ optional '-dirty' suffix)
	utcTime         string      // Build time in UTC (year/month/day hour:min:sec)
	rev             string      // SHA-1 of this build (git rev-parse)
	cgoCompiler     = cgoVersion()
	cgoTargetTriple string
	platform        = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
	// Distribution is set to empty by default after open source
	Distribution = ""
	typ          string // Type of this build: <empty>, "development", or "release[-gnu|-musl]"
	channel      = "unknown"
	envChannel   = envutil.EnvOrDefaultString("KWBASE_CHANNEL", "unknown")
)

// IsRelease returns true if the binary was produced by a "release" build.
func IsRelease() bool {
	return typ == "release"
}

// SeemsOfficial reports whether this binary is likely to have come from an
// official release channel.
func SeemsOfficial() bool {
	return channel == "official-binary" || channel == "source-archive"
}

// VersionPrefix returns the version prefix of the current build.
func VersionPrefix() string {
	v, err := version.Parse(tag)
	if err != nil {
		return "dev"
	}
	return fmt.Sprintf("V%d.%d", v.Major(), v.Minor())
}

func init() {
	// Allow tests to override the tag.
	if tagOverride := envutil.EnvOrDefaultString(
		"KWBASE_TESTING_VERSION_TAG", ""); tagOverride != "" {
		tag = tagOverride
	}
}

// Short returns a pretty printed build and version summary.
func (b Info) Short() string {
	plat := b.Platform
	if b.CgoTargetTriple != "" {
		plat = b.CgoTargetTriple
	}
	// Distribution is set to "" so we concat it with tag here to make
	// the default output look better
	return fmt.Sprintf("KaiwuDB %s (%s, built %s, %s, %s)",
		b.Distribution+b.Tag, plat, b.Time, b.GoVersion, b.CgoCompiler)
}

// GoTime parses the utcTime string and returns a time.Time.
func (b Info) GoTime() time.Time {
	val, err := time.Parse(TimeFormat, b.Time)
	if err != nil {
		return time.Time{}
	}
	return val
}

// Timestamp parses the utcTime string and returns the number of seconds since epoch.
func (b Info) Timestamp() (int64, error) {
	val, err := time.Parse(TimeFormat, b.Time)
	if err != nil {
		return 0, err
	}
	return val.Unix(), nil
}

// GetInfo returns an Info struct populated with the build information.
func GetInfo() Info {
	return Info{
		GoVersion:       runtime.Version(),
		Tag:             tag,
		Time:            utcTime,
		Revision:        rev,
		CgoCompiler:     cgoCompiler,
		CgoTargetTriple: cgoTargetTriple,
		Platform:        platform,
		Distribution:    Distribution,
		Type:            typ,
		Channel:         channel,
		EnvChannel:      envChannel,
	}
}

// TestingOverrideTag allows tests to override the build tag.
func TestingOverrideTag(t string) func() {
	prev := tag
	tag = t
	return func() { tag = prev }
}
