// Copyright 2016 The Cockroach Authors.
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

package util

import (
	"unicode/utf8"

	"github.com/pkg/errors"
)

// GetSingleRune decodes the string s as a single rune if possible.
func GetSingleRune(s string) (rune, error) {
	if s == "" {
		return 0, nil
	}
	r, sz := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return 0, errors.Errorf("invalid character: %s", s)
	}
	if sz != len(s) {
		return r, errors.New("must be only one character")
	}
	return r, nil
}

// TruncateString truncates a string to a given number of runes.
func TruncateString(s string, maxRunes int) string {
	// This is a fast path (len(s) is an upper bound for RuneCountInString).
	if len(s) <= maxRunes {
		return s
	}
	n := utf8.RuneCountInString(s)
	if n <= maxRunes {
		return s
	}
	// Fast path for ASCII strings.
	if len(s) == n {
		return s[:maxRunes]
	}
	i := 0
	for pos := range s {
		if i == maxRunes {
			return s[:pos]
		}
		i++
	}
	// This code should be unreachable.
	return s
}
