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

// BoolSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bool" is updated.
type BoolSetting struct {
	common
	defaultValue bool
}

var _ extendedSetting = &BoolSetting{}

// Get retrieves the bool value in the setting.
func (b *BoolSetting) Get(sv *Values) bool {
	return sv.getInt64(b.slotIdx) != 0
}

func (b *BoolSetting) String(sv *Values) string {
	return EncodeBool(b.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (b *BoolSetting) Encoded(sv *Values) string {
	return b.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (b *BoolSetting) EncodedDefault() string {
	return EncodeBool(b.defaultValue)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*BoolSetting) Typ() string {
	return "b"
}

// Default returns default value for setting.
func (b *BoolSetting) Default() bool {
	return b.defaultValue
}

// Defeat the linter.
var _ = (*BoolSetting).Default

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (b *BoolSetting) Override(sv *Values, v bool) {
	b.set(sv, v)

	vInt := int64(0)
	if v {
		vInt = 1
	}
	sv.setDefaultOverrideInt64(b.slotIdx, vInt)
}

func (b *BoolSetting) set(sv *Values, v bool) {
	vInt := int64(0)
	if v {
		vInt = 1
	}
	sv.setInt64(b.slotIdx, vInt)
}

func (b *BoolSetting) setToDefault(sv *Values) {
	// See if the default value was overridden.
	ok, val, _ := sv.getDefaultOverride(b.slotIdx)
	if ok {
		b.set(sv, val > 0)
		return
	}
	b.set(sv, b.defaultValue)
}

// RegisterBoolSetting defines a new setting with type bool.
func RegisterBoolSetting(key, desc string, defaultValue bool) *BoolSetting {
	setting := &BoolSetting{defaultValue: defaultValue}
	register(key, desc, setting)
	return setting
}

// RegisterPublicBoolSetting defines a new setting with type bool and makes it public.
func RegisterPublicBoolSetting(key, desc string, defaultValue bool) *BoolSetting {
	s := RegisterBoolSetting(key, desc, defaultValue)
	s.SetVisibility(Public)
	return s
}
