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

// Package flagutil facilitates creation of rich flag types.
package flagutil

import (
	"fmt"
	"regexp"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/spf13/pflag"
)

// TimeFormats are the time formats which a time flag accepts for parsing time
// as described in time.Parse.
var TimeFormats = []string{
	log.MessageTimeFormat,
	log.FileTimeFormat,
	time.RFC3339Nano,
	time.RFC3339,
	time.Kitchen,
	"15:04:00.999999Z",
	"15:04:00.999999",
	"15:04Z",
	"15:04",
}

// Time returns a value which can be used with pflag.Var to create flags for
// time variables. The flag attempts to parse its in a variety of formats.
// The first is as a duration using time.ParseDuration and subtracting the
// result from Now() and then by using the values in Formats for parsing using
// time.Parse. An empty flag sets t to the zero value.
func Time(t *time.Time) pflag.Value {
	return (*timeFlag)(t)
}

// Regexp returns a value which can be used with pflag.Var to create flags
// for regexp variables. The flag attempts to compile its input to a regular
// expression and assigns the result to *r.
// If the flag is empty, r is set to nil.
func Regexp(r **regexp.Regexp) pflag.Value {
	return re{re: r}
}

// timeFlag implements pflag.Value and enables easy creation of time flags.
type timeFlag time.Time

func (f *timeFlag) String() string {
	if (*time.Time)(f).IsZero() {
		return ""
	}
	return (*time.Time)(f).Format(log.MessageTimeFormat)
}

func (f *timeFlag) Type() string { return "time" }

func (f *timeFlag) Set(s string) error {
	if s == "" {
		*f = (timeFlag)(time.Time{})
		return nil
	}
	for _, p := range parsers {
		if t, err := p.parseTime(s); err == nil {
			*f = timeFlag(t.UTC())
			return nil
		}
	}
	return fmt.Errorf("failed to parse %q as time", s)
}

var parsers = func() (parsers []interface {
	parseTime(s string) (time.Time, error)
}) {
	parsers = append(parsers, durationParser(time.ParseDuration))
	for _, p := range TimeFormats {
		parsers = append(parsers, formatParser(p))
	}
	return parsers
}()

type formatParser string

func (f formatParser) parseTime(s string) (time.Time, error) {
	return time.Parse(string(f), s)
}

type durationParser func(string) (time.Duration, error)

func (f durationParser) parseTime(s string) (time.Time, error) {
	d, err := f(s)
	if err != nil {
		return time.Time{}, err
	}
	return timeutil.Now().Add(-1 * d), nil
}

type re struct {
	re **regexp.Regexp
}

func (r re) String() string {
	if *r.re == nil {
		return ""
	}
	return (*r.re).String()
}

func (r re) Set(s string) error {
	if s == "" {
		*r.re = nil
		return nil
	}
	re, err := regexp.Compile(s)
	if err != nil {
		return err
	}
	*r.re = re
	return nil
}

func (r re) Type() string { return "regexp" }
