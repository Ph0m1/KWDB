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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package pprofui

import (
	"io"
	"net/http"
)

// responseBridge is a helper for fetching from the pprof profile handlers.
// Their interface wants a http.ResponseWriter, so we give it one. The writes
// are passed through to an `io.Writer` of our choosing.
type responseBridge struct {
	target     io.Writer
	statusCode int
}

var _ http.ResponseWriter = &responseBridge{}

func (r *responseBridge) Header() http.Header {
	return http.Header{}
}

func (r *responseBridge) Write(b []byte) (int, error) {
	return r.target.Write(b)
}

func (r *responseBridge) WriteHeader(statusCode int) {
	r.statusCode = statusCode
}
