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

package pprofui

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"net/url"
	"path"
	runtimepprof "runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/google/pprof/driver"
	"github.com/google/pprof/profile"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// A Server serves up the pprof web ui. A request to /<profiletype>
// generates a profile of the desired type and redirects to the UI for
// it at /<profiletype>/<id>. Valid profile types at the time of
// writing include `profile` (cpu), `goroutine`, `threadcreate`,
// `heap`, `block`, and `mutex`.
type Server struct {
	storage      Storage
	profileSem   syncutil.Mutex
	profileTypes map[string]http.HandlerFunc
	hook         func(profile string, labels bool, do func())
}

// NewServer creates a new Server backed by the supplied Storage and optionally
// a hook which is called when a new profile is created. The closure passed to
// the hook will carry out the work involved in creating the profile and must
// be called by the hook. The intention is that hook will be a method such as
// this:
//
// func hook(profile string, do func()) {
// 	if profile == "profile" {
// 		something.EnableProfilerLabels()
// 		defer something.DisableProfilerLabels()
// 		do()
// 	}
// }
func NewServer(storage Storage, hook func(profile string, labels bool, do func())) *Server {
	if hook == nil {
		hook = func(_ string, _ bool, do func()) { do() }
	}
	s := &Server{
		storage: storage,
		hook:    hook,
	}

	s.profileTypes = map[string]http.HandlerFunc{
		// The CPU profile endpoint is special in that the handler actually blocks
		// for a predetermined duration (recording the profile in the meantime).
		// It is not included in `runtimepprof.Profiles` below.
		"profile": func(w http.ResponseWriter, r *http.Request) {
			const defaultProfileDurationSeconds = 5
			if r.Form == nil {
				r.Form = url.Values{}
			}
			if r.Form.Get("seconds") == "" {
				r.Form.Set("seconds", strconv.Itoa(defaultProfileDurationSeconds))
			}
			s.profileSem.Lock()
			defer s.profileSem.Unlock()
			pprof.Profile(w, r)
		},
	}

	// Register the endpoints for heap, block, threadcreate, etc.
	for _, p := range runtimepprof.Profiles() {
		p := p // copy
		s.profileTypes[p.Name()] = func(w http.ResponseWriter, r *http.Request) {
			if err := p.WriteTo(w, 0 /* debug */); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(err.Error()))
			}
		}
	}

	return s
}

// parsePath turns /profile/123/flamegraph/banana into (profile, 123, /flamegraph/banana).
func (s *Server) parsePath(reqPath string) (profType string, id string, remainingPath string) {
	parts := strings.Split(path.Clean(reqPath), "/")
	if parts[0] == "" {
		// The path was absolute (the typical case), pretend it was
		// relative (to this handler's root).
		parts = parts[1:]
	}
	switch len(parts) {
	case 0:
		return "", "", "/"
	case 1:
		return parts[0], "", "/"
	default:
		return parts[0], parts[1], "/" + strings.Join(parts[2:], "/")
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	profileName, id, remainingPath := s.parsePath(r.URL.Path)

	if profileName == "" {
		// TODO(tschottdorf): serve an overview page.
		var names []string
		for name := range s.profileTypes {
			names = append(names, name)
		}
		sort.Strings(names)
		msg := fmt.Sprintf("Try %s for one of %s", path.Join(r.RequestURI, "<profileName>"), strings.Join(names, ", "))
		http.Error(w, msg, http.StatusNotFound)
		return
	}

	if id != "" {
		// Catch nonexistent IDs early or pprof will do a worse job at
		// giving an informative error.
		if err := s.storage.Get(id, func(io.Reader) error { return nil }); err != nil {
			msg := fmt.Sprintf("profile for id %s not found: %s", id, err)
			http.Error(w, msg, http.StatusNotFound)
			return
		}

		if r.URL.Query().Get("download") != "" {
			// TODO(tbg): this has zero discoverability.
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s_%s.pb.gz", profileName, id))
			w.Header().Set("Content-Type", "application/octet-stream")
			if err := s.storage.Get(id, func(r io.Reader) error {
				_, err := io.Copy(w, r)
				return err
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		server := func(args *driver.HTTPServerArgs) error {
			handler, ok := args.Handlers[remainingPath]
			if !ok {
				return errors.Errorf("unknown endpoint %s", remainingPath)
			}
			handler.ServeHTTP(w, r)
			return nil
		}

		storageFetcher := func(_ string, _, _ time.Duration) (*profile.Profile, string, error) {
			var p *profile.Profile
			if err := s.storage.Get(id, func(reader io.Reader) error {
				var err error
				p, err = profile.Parse(reader)
				return err
			}); err != nil {
				return nil, "", err
			}
			return p, "", nil
		}

		// Invoke the (library version) of `pprof` with a number of stubs.
		// Specifically, we pass a fake FlagSet that plumbs through the
		// given args, a UI that logs any errors pprof may emit, a fetcher
		// that simply reads the profile we downloaded earlier, and a
		// HTTPServer that pprof will pass the web ui handlers to at the
		// end (and we let it handle this client request).
		if err := driver.PProf(&driver.Options{
			Flagset: &pprofFlags{
				FlagSet: pflag.NewFlagSet("pprof", pflag.ExitOnError),
				args: []string{
					"--symbolize", "none",
					"--http", "localhost:0",
					"", // we inject our own target
				},
			},
			UI:         &fakeUI{},
			Fetch:      fetcherFn(storageFetcher),
			HTTPServer: server,
		}); err != nil {
			_, _ = w.Write([]byte(err.Error()))
		}

		return
	}

	// Create and save new profile, then redirect client to corresponding ui URL.

	id = s.storage.ID()

	fetchHandler, ok := s.profileTypes[profileName]
	if !ok {
		_, _ = w.Write([]byte(fmt.Sprintf("unknown profile type %s", profileName)))
		return
	}

	if err := s.storage.Store(id, func(w io.Writer) error {
		req, err := http.NewRequest("GET", "/unused", bytes.NewReader(nil))
		if err != nil {
			return err
		}

		// Pass through any parameters. Most notably, allow ?seconds=10 for
		// CPU profiles.
		_ = r.ParseForm()
		req.Form = r.Form

		rw := &responseBridge{target: w}

		s.hook(profileName, r.Form.Get("labels") != "", func() { fetchHandler(rw, req) })

		if rw.statusCode != http.StatusOK && rw.statusCode != 0 {
			return errors.Errorf("unexpected status: %d", rw.statusCode)
		}
		return nil
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// NB: direct straight to the flamegraph. This is because `pprof`
	// shells out to `dot` for the default landing page and thus works
	// only on hosts that have graphviz installed. You can still navigate
	// to the dot page from there.
	origURL, err := url.Parse(r.RequestURI)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// If this is a request issued by `go tool pprof`, just return the profile
	// directly. This is convenient because it avoids having to expose the pprof
	// endpoints separately, and also allows inserting hooks around CPU profiles
	// in the future.
	isGoPProf := strings.Contains(r.Header.Get("User-Agent"), "Go-http-client")
	origURL.Path = path.Join(origURL.Path, id, "flamegraph")
	if !isGoPProf {
		http.Redirect(w, r, origURL.String(), http.StatusTemporaryRedirect)
	} else {
		_ = s.storage.Get(id, func(r io.Reader) error {
			_, err := io.Copy(w, r)
			return err
		})
	}
}

type fetcherFn func(_ string, _, _ time.Duration) (*profile.Profile, string, error)

func (f fetcherFn) Fetch(s string, d, t time.Duration) (*profile.Profile, string, error) {
	return f(s, d, t)
}
