// Copyright 2018 The Cockroach Authors.
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

package pgdate

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Various keywords that appear in timestamps.
const (
	// Alias for UTC.  No idea who actually uses this.
	keywordAllBalls = "allballs"
	keywordAM       = "am"
	keywordEpoch    = "epoch"
	keywordEraAD    = "ad"
	keywordEraBC    = "bc"
	keywordEraBCE   = "bce"
	keywordEraCE    = "ce"
	// Alias for UTC.
	keywordGMT       = "gmt"
	keywordInfinity  = "infinity"
	keywordNow       = "now"
	keywordPM        = "pm"
	keywordToday     = "today"
	keywordTomorrow  = "tomorrow"
	keywordUTC       = "utc"
	keywordYesterday = "yesterday"
	// Alias for UTC.
	keywordZ = "z"
	// Alias for UTC.
	keywordZulu = "zulu"
)

// Commonly-used collections of fields.
var (
	dateFields         = newFieldSet(fieldYear, fieldMonth, fieldDay, fieldEra)
	dateRequiredFields = newFieldSet(fieldYear, fieldMonth, fieldDay)

	timeFields = newFieldSet(
		fieldHour, fieldMinute, fieldSecond, fieldNanos, fieldMeridian,
		fieldTZHour, fieldTZMinute, fieldTZSecond)
	timeRequiredFields = newFieldSet(fieldHour, fieldMinute)

	dateTimeFields = dateFields.AddAll(timeFields)

	tzFields = newFieldSet(fieldTZHour, fieldTZMinute, fieldTZSecond)
)

// These are sentinel values for handling special values:
// https://www.postgresql.org/docs/10/static/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-TABLE
var (
	TimeEpoch = timeutil.Unix(0, 0)
	// TimeInfinity represents the "highest" possible time.
	// TODO (#41564): this should actually behave as infinity, i.e. any operator
	// leaves this as infinity. This time should always be greater than any other time.
	// We should probably use the next microsecond after this value, i.e. timeutil.Unix(9224318016000, 0).
	// Postgres uses math.MaxInt64 microseconds as the infinity value.
	// See: https://github.com/postgres/postgres/blob/42aa1f0ab321fd43cbfdd875dd9e13940b485900/src/include/datatype/timestamp.h#L107.
	TimeInfinity = timeutil.Unix(9224318016000-1, 999999000)
	// TimeNegativeInfinity represents the "lowest" possible time.
	// TODO (#41564): this should actually behave as -infinity, i.e. any operator
	// leaves this as -infinity. This time should always be less than any other time.
	// We should probably use the next microsecond before this value, i.e. timeutil.Unix(9224318016000-1, 999999000).
	// Postgres uses math.MinInt64 microseconds as the -infinity value.
	// See: https://github.com/postgres/postgres/blob/42aa1f0ab321fd43cbfdd875dd9e13940b485900/src/include/datatype/timestamp.h#L107.
	TimeNegativeInfinity = timeutil.Unix(-210866803200, 0)
)

//go:generate stringer -type=ParseMode

// ParseMode controls the resolution of ambiguous date formats such as
// `01/02/03`.
type ParseMode uint

// These are the various parsing modes that determine in which order
// we should look for years, months, and date.
// ParseModeYMD is the default value.
const (
	ParseModeYMD ParseMode = iota
	ParseModeDMY
	ParseModeMDY
)

// ParseDate converts a string into Date.
func ParseDate(now time.Time, mode ParseMode, s string) (Date, error) {
	fe := fieldExtract{
		now:      now,
		mode:     mode,
		required: dateRequiredFields,
		// We allow time fields to be provided since they occur after
		// the date fields that we're really looking for and for
		// time values like 24:00:00, would push into the next day.
		wanted: dateTimeFields,
	}

	if err := fe.Extract(s); err != nil {
		return Date{}, parseError(err, "date", s)
	}
	return fe.MakeDate()
}

// ParseTime converts a string into a time value on the epoch day.
func ParseTime(now time.Time, mode ParseMode, s string) (time.Time, error) {
	fe := fieldExtract{
		now:      now,
		required: timeRequiredFields,
		wanted:   timeFields,
	}

	if err := fe.Extract(s); err != nil {
		// It's possible that the user has given us a complete
		// timestamp string; let's try again, accepting more fields.
		fe = fieldExtract{
			now:      now,
			mode:     mode,
			required: timeRequiredFields,
			wanted:   dateTimeFields,
		}

		if err := fe.Extract(s); err != nil {
			return TimeEpoch, parseError(err, "time", s)
		}
	}
	return fe.MakeTime(), nil
}

// ParseTimestamp converts a string into a timestamp.
func ParseTimestamp(now time.Time, mode ParseMode, s string) (time.Time, error) {
	fe := fieldExtract{
		mode: mode,
		now:  now,
		// A timestamp only actually needs a date component; the time
		// would be midnight.
		required: dateRequiredFields,
		wanted:   dateTimeFields,
	}

	if err := fe.Extract(s); err != nil {
		return TimeEpoch, parseError(err, "timestamp", s)
	}
	return fe.MakeTimestamp(), nil
}

// badFieldPrefixError constructs an error with pg code InvalidDatetimeFormat.
func badFieldPrefixError(field field, prefix rune) error {
	return inputErrorf("unexpected separator '%s' for field %s", string(prefix), errors.Safe(field.Pretty()))
}

// inputErrorf returns an error with pg code InvalidDatetimeFormat.
func inputErrorf(format string, args ...interface{}) error {
	return pgerror.WithCandidateCode(errors.Newf(format, args...), pgcode.InvalidDatetimeFormat)
}

// outOfRangeError returns an error with pg code DatetimeFieldOverflow.
func outOfRangeError(field string, val int) error {
	return pgerror.WithCandidateCode(
		errors.Newf("field %s value %d is out of range", errors.Safe(field), errors.Safe(val)),
		pgcode.DatetimeFieldOverflow)
}

// parseError ensures that any error we return to the client will
// be some kind of error with a pg code.
func parseError(err error, kind string, s string) error {
	return pgerror.WithCandidateCode(
		errors.Wrapf(err, "parsing as type %s", errors.Safe(kind)),
		pgcode.InvalidDatetimeFormat)
}
