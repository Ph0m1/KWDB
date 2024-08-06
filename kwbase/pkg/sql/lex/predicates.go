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

package lex

import (
	"unicode"
	"unicode/utf8"
)

// isASCII returns true if all the characters in s are ASCII.
func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// IsDigit returns true if the character is between 0 and 9.
func IsDigit(ch int) bool {
	return ch >= '0' && ch <= '9'
}

// IsHexDigit returns true if the character is a valid hexadecimal digit.
func IsHexDigit(ch int) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'f') ||
		(ch >= 'A' && ch <= 'F')
}

// reservedOrLookaheadKeywords are the reserved keywords plus those keywords for
// which we need one token of lookahead extra to determine their token type.
var reservedOrLookaheadKeywords = make(map[string]struct{})

func init() {
	for s := range reservedKeywords {
		reservedOrLookaheadKeywords[s] = struct{}{}
	}
	for _, s := range []string{
		"between",
		"ilike",
		"in",
		"like",
		"of",
		"ordinality",
		"similar",
		"time",
	} {
		reservedOrLookaheadKeywords[s] = struct{}{}
	}
}

// isReservedKeyword returns true if the keyword is reserved, or needs
// one extra token of lookahead.
func isReservedKeyword(s string) bool {
	_, ok := reservedOrLookaheadKeywords[s]
	return ok
}

// isBareIdentifier returns true if the input string is a permissible bare SQL
// identifier.
func isBareIdentifier(s string) bool {
	if len(s) == 0 || !IsIdentStart(int(s[0])) || (s[0] >= 'A' && s[0] <= 'Z') {
		return false
	}
	// Keep track of whether the input string is all ASCII. If it is, we don't
	// have to bother running the full Normalize() function at the end, which is
	// quite expensive.
	isASCII := s[0] < utf8.RuneSelf
	for i := 1; i < len(s); i++ {
		if !IsIdentMiddle(int(s[i])) {
			return false
		}
		if s[i] >= 'A' && s[i] <= 'Z' {
			// Non-lowercase identifiers aren't permissible.
			return false
		}
		if s[i] >= utf8.RuneSelf {
			isASCII = false
		}
	}
	return isASCII || NormalizeName(s) == s
}

// IsIdentStart returns true if the character is valid at the start of an identifier.
func IsIdentStart(ch int) bool {
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 128 && ch <= 255) ||
		(ch == '_')
}

// IsIdentMiddle returns true if the character is valid inside an identifier.
func IsIdentMiddle(ch int) bool {
	return IsIdentStart(ch) || IsDigit(ch) || ch == '$'
}

// CheckNameValid returns true if the name is valid.
func CheckNameValid(name string) bool {
	return !isReservedKeyword(name) && isBareIdentifier(name)
}
