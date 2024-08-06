// Copyright 2019 The Cockroach Authors.
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

package httputil

import (
	"context"
	"io"
	"net"
	"net/http"
	"time"
)

// DefaultClient is a replacement for http.DefaultClient which defines
// a standard timeout.
var DefaultClient = NewClientWithTimeout(standardHTTPTimeout)

// DiagramsClient is a replacement for http.DefaultClient which defines
// a generate diagrams timeout.
var DiagramsClient = NewClientWithTimeout(diagramsHTTPTimeout)

const standardHTTPTimeout time.Duration = 3 * time.Second

const diagramsHTTPTimeout time.Duration = 60 * time.Second

// NewClientWithTimeout defines a http.Client with the given timeout.
func NewClientWithTimeout(timeout time.Duration) *Client {
	return &Client{&http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			// Don't leak a goroutine on OSX (the TCP level timeout is probably
			// much higher than on linux).
			DialContext:       (&net.Dialer{Timeout: timeout}).DialContext,
			DisableKeepAlives: true,
		},
	}}
}

// Client is a replacement for http.Client which implements method
// variants that respect a provided context's cancellation status.
type Client struct {
	*http.Client
}

// Get does like http.Get but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Get(ctx context.Context, url string) (resp *http.Response, err error) {
	return DefaultClient.Get(ctx, url)
}

// Head does like http.Head but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Head(ctx context.Context, url string) (resp *http.Response, err error) {
	return DefaultClient.Head(ctx, url)
}

// Post does like http.Post but uses the provided context and obeys its cancellation.
// It also uses the default client with a default 3 second timeout.
func Post(
	ctx context.Context, url, contentType string, body io.Reader,
) (resp *http.Response, err error) {
	return DefaultClient.Post(ctx, url, contentType, body)
}

// DiagramsPost does like http.Post but uses the provided context and obeys its cancellation.
// It also uses the generate diagrams client with a default 120 second timeout.
func DiagramsPost(
	ctx context.Context, url, contentType string, body io.Reader,
) (resp *http.Response, err error) {
	return DiagramsClient.Post(ctx, url, contentType, body)
}

// Get does like http.Client.Get but uses the provided context and obeys its cancellation.
func (c *Client) Get(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Head does like http.Client.Head but uses the provided context and obeys its cancellation.
func (c *Client) Head(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Post does like http.Client.Post but uses the provided context and obeys its cancellation.
func (c *Client) Post(
	ctx context.Context, url, contentType string, body io.Reader,
) (resp *http.Response, err error) {
	req, err := NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	return c.Do(req)
}
