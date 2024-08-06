// Copyright 2017 The Cockroach Authors.
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
	"math"

	"github.com/pkg/errors"
)

// FloatSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "float" is updated.
type FloatSetting struct {
	common
	defaultValue float64
	validateFn   func(float64) error
}

var _ extendedSetting = &FloatSetting{}

// Get retrieves the float value in the setting.
func (f *FloatSetting) Get(sv *Values) float64 {
	return math.Float64frombits(uint64(sv.getInt64(f.slotIdx)))
}

func (f *FloatSetting) String(sv *Values) string {
	return EncodeFloat(f.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (f *FloatSetting) Encoded(sv *Values) string {
	return f.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (f *FloatSetting) EncodedDefault() string {
	return EncodeFloat(f.defaultValue)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*FloatSetting) Typ() string {
	return "f"
}

// Defeat the linter.
var _ = (*FloatSetting).Default

// Override changes the setting panicking if validation fails and also overrides
// the default value.
//
// For testing usage only.
func (f *FloatSetting) Override(sv *Values, v float64) {
	if err := f.set(sv, v); err != nil {
		panic(err)
	}
	sv.setDefaultOverrideInt64(f.slotIdx, int64(math.Float64bits(v)))
}

// Validate that a value conforms with the validation function.
func (f *FloatSetting) Validate(v float64) error {
	if f.validateFn != nil {
		if err := f.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

func (f *FloatSetting) set(sv *Values, v float64) error {
	if err := f.Validate(v); err != nil {
		return err
	}
	sv.setInt64(f.slotIdx, int64(math.Float64bits(v)))
	return nil
}

func (f *FloatSetting) setToDefault(sv *Values) {
	// See if the default value was overridden.
	ok, val, _ := sv.getDefaultOverride(f.slotIdx)
	if ok {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = f.set(sv, math.Float64frombits(uint64((val))))
		return
	}
	if err := f.set(sv, f.defaultValue); err != nil {
		panic(err)
	}
}

// Default returns the default value.
func (f *FloatSetting) Default() float64 {
	return f.defaultValue
}

// RegisterFloatSetting defines a new setting with type float.
func RegisterFloatSetting(key, desc string, defaultValue float64) *FloatSetting {
	return RegisterValidatedFloatSetting(key, desc, defaultValue, nil)
}

// RegisterNonNegativeFloatSetting defines a new setting with type float.
func RegisterNonNegativeFloatSetting(key, desc string, defaultValue float64) *FloatSetting {
	return RegisterValidatedFloatSetting(key, desc, defaultValue, func(v float64) error {
		if v < 0 {
			return errors.Errorf("cannot set %s to a negative value: %f", key, v)
		}
		return nil
	})
}

// RegisterValidatedFloatSetting defines a new setting with type float.
func RegisterValidatedFloatSetting(
	key, desc string, defaultValue float64, validateFn func(float64) error,
) *FloatSetting {
	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &FloatSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}
