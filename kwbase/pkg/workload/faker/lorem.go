// Copyright 2019 The Cockroach Authors.
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

package faker

import (
	"strings"
	"unicode"

	"golang.org/x/exp/rand"
)

type loremFaker struct {
	words *weightedEntries
}

// Words returns the requested number of random en_US words.
func (f *loremFaker) Words(rng *rand.Rand, num int) []string {
	w := make([]string, num)
	for i := range w {
		w[i] = f.words.Rand(rng).(string)
	}
	return w
}

// Sentences returns the requested number of random en_US sentences.
func (f *loremFaker) Sentences(rng *rand.Rand, num int) []string {
	s := make([]string, num)
	for i := range s {
		var b strings.Builder
		numWords := randInt(rng, 4, 8)
		for j := 0; j < numWords; j++ {
			word := f.words.Rand(rng).(string)
			if j == 0 {
				word = firstToUpper(word)
			}
			b.WriteString(word)
			if j == numWords-1 {
				b.WriteString(`.`)
			} else {
				b.WriteString(` `)
			}
		}
		s[i] = b.String()
	}
	return s
}

func firstToUpper(s string) string {
	isFirst := true
	return strings.Map(func(r rune) rune {
		if isFirst {
			isFirst = false
			return unicode.ToUpper(r)
		}
		return r
	}, s)
}

// Paragraph returns a random en_US paragraph.
func (f *loremFaker) Paragraph(rng *rand.Rand) string {
	return strings.Join(f.Sentences(rng, randInt(rng, 1, 5)), ` `)
}

func newLoremFaker() loremFaker {
	f := loremFaker{}
	f.words = words()
	return f
}
