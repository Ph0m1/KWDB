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

import "github.com/pkg/errors"

// IntSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct {
	common
	defaultValue int64
	validateFn   func(int64) error
}

var _ extendedSetting = &IntSetting{}

// Get retrieves the int value in the setting.
func (i *IntSetting) Get(sv *Values) int64 {
	return sv.container.getInt64(i.slotIdx)
}

func (i *IntSetting) String(sv *Values) string {
	return EncodeInt(i.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (i *IntSetting) Encoded(sv *Values) string {
	return i.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (i *IntSetting) EncodedDefault() string {
	return EncodeInt(i.defaultValue)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*IntSetting) Typ() string {
	return "i"
}

// Defeat the linter.
var _ = (*IntSetting).Default

// Validate that a value conforms with the validation function.
func (i *IntSetting) Validate(v int64) error {
	if i.validateFn != nil {
		if err := i.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (i *IntSetting) Override(sv *Values, v int64) {
	sv.setInt64(i.slotIdx, v)
	sv.setDefaultOverrideInt64(i.slotIdx, v)
}

func (i *IntSetting) set(sv *Values, v int64) error {
	if err := i.Validate(v); err != nil {
		return err
	}
	sv.setInt64(i.slotIdx, v)
	return nil
}

func (i *IntSetting) setToDefault(sv *Values) {
	// See if the default value was overridden.
	ok, val, _ := sv.getDefaultOverride(i.slotIdx)
	if ok {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = i.set(sv, val)
		return
	}
	if err := i.set(sv, i.defaultValue); err != nil {
		panic(err)
	}
}

// Default returns the default value.
func (i *IntSetting) Default() int64 {
	return i.defaultValue
}

// RegisterIntSetting defines a new setting with type int.
func RegisterIntSetting(key, desc string, defaultValue int64) *IntSetting {
	return RegisterValidatedIntSetting(key, desc, defaultValue, nil)
}

// RegisterPublicIntSetting defines a new setting with type int and makes it public.
func RegisterPublicIntSetting(key, desc string, defaultValue int64) *IntSetting {
	s := RegisterValidatedIntSetting(key, desc, defaultValue, nil)
	s.SetVisibility(Public)
	return s
}

// RegisterNonNegativeIntSetting defines a new setting with type int.
func RegisterNonNegativeIntSetting(key, desc string, defaultValue int64) *IntSetting {
	return RegisterValidatedIntSetting(key, desc, defaultValue, func(v int64) error {
		if v < 0 {
			return errors.Errorf("cannot set %s to a negative value: %d", key, v)
		}
		return nil
	})
}

// RegisterPositiveIntSetting defines a new setting with type int.
func RegisterPositiveIntSetting(key, desc string, defaultValue int64) *IntSetting {
	return RegisterValidatedIntSetting(key, desc, defaultValue, func(v int64) error {
		if v < 1 {
			return errors.Errorf("cannot set %s to a value < 1: %d", key, v)
		}
		return nil
	})
}

// RegisterPublicValidatedIntSetting defines a new setting with type int and makes it public
// with a validation function.
func RegisterPublicValidatedIntSetting(
	key, desc string, defaultValue int64, validateFn func(int64) error,
) *IntSetting {
	s := RegisterValidatedIntSetting(key, desc, defaultValue, validateFn)
	s.SetVisibility(Public)
	return s
}

// RegisterValidatedIntSetting defines a new setting with type int with a
// validation function.
func RegisterValidatedIntSetting(
	key, desc string, defaultValue int64, validateFn func(int64) error,
) *IntSetting {
	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &IntSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}
