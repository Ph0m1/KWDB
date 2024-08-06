// Copyright 2017 The Cockroach Authors.
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

package settings

import (
	"time"

	"github.com/cockroachdb/errors"
)

// DurationSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "duration" is updated.
type DurationSetting struct {
	common
	defaultValue time.Duration
	validateFn   func(time.Duration) error
}

var _ extendedSetting = &DurationSetting{}

// Get retrieves the duration value in the setting.
func (d *DurationSetting) Get(sv *Values) time.Duration {
	return time.Duration(sv.getInt64(d.slotIdx))
}

func (d *DurationSetting) String(sv *Values) string {
	return EncodeDuration(d.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (d *DurationSetting) Encoded(sv *Values) string {
	return d.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (d *DurationSetting) EncodedDefault() string {
	return EncodeDuration(d.defaultValue)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*DurationSetting) Typ() string {
	return "d"
}

// Default returns default value for setting.
func (d *DurationSetting) Default() time.Duration {
	return d.defaultValue
}

// Defeat the linter.
var _ = (*DurationSetting).Default

// Validate that a value conforms with the validation function.
func (d *DurationSetting) Validate(v time.Duration) error {
	if d.validateFn != nil {
		if err := d.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (d *DurationSetting) Override(sv *Values, v time.Duration) {
	sv.setInt64(d.slotIdx, int64(v))
	sv.setDefaultOverrideInt64(d.slotIdx, int64(v))
}

func (d *DurationSetting) set(sv *Values, v time.Duration) error {
	if err := d.Validate(v); err != nil {
		return err
	}
	sv.setInt64(d.slotIdx, int64(v))
	return nil
}

func (d *DurationSetting) setToDefault(sv *Values) {
	// See if the default value was overridden.
	ok, val, _ := sv.getDefaultOverride(d.slotIdx)
	if ok {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = d.set(sv, time.Duration(val))
		return
	}
	if err := d.set(sv, d.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterDurationSetting defines a new setting with type duration.
func RegisterDurationSetting(key, desc string, defaultValue time.Duration) *DurationSetting {
	return RegisterValidatedDurationSetting(key, desc, defaultValue, nil)
}

// RegisterPublicDurationSetting defines a new setting with type
// duration and makes it public.
func RegisterPublicDurationSetting(key, desc string, defaultValue time.Duration) *DurationSetting {
	s := RegisterValidatedDurationSetting(key, desc, defaultValue, nil)
	s.SetVisibility(Public)
	return s
}

// RegisterPublicNonNegativeDurationSetting defines a new setting with
// type duration and makes it public.
func RegisterPublicNonNegativeDurationSetting(
	key, desc string, defaultValue time.Duration,
) *DurationSetting {
	s := RegisterNonNegativeDurationSetting(key, desc, defaultValue)
	s.SetVisibility(Public)
	return s
}

// RegisterPublicNonNegativeDurationSettingWithMaximum defines a new setting with
// type duration, makes it public, and sets a maximum value.
// The maximum value is an allowed value.
func RegisterPublicNonNegativeDurationSettingWithMaximum(
	key, desc string, defaultValue time.Duration, maxValue time.Duration,
) *DurationSetting {
	s := RegisterValidatedDurationSetting(key, desc, defaultValue, func(v time.Duration) error {
		if v < 0 {
			return errors.Errorf("cannot set %s to a negative duration: %s", key, v)
		}
		if v > maxValue {
			return errors.Errorf("cannot set %s to a value larger than %s", key, maxValue)
		}
		return nil
	})
	s.SetVisibility(Public)
	return s
}

// RegisterNonNegativeDurationSetting defines a new setting with type duration.
func RegisterNonNegativeDurationSetting(
	key, desc string, defaultValue time.Duration,
) *DurationSetting {
	return RegisterValidatedDurationSetting(key, desc, defaultValue, func(v time.Duration) error {
		if v < 0 {
			return errors.Errorf("cannot set %s to a negative duration: %s", key, v)
		}
		return nil
	})
}

// RegisterValidatedDurationSetting defines a new setting with type duration.
func RegisterValidatedDurationSetting(
	key, desc string, defaultValue time.Duration, validateFn func(time.Duration) error,
) *DurationSetting {
	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &DurationSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}
