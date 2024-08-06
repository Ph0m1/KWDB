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

package pgtest

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/jackc/pgx/pgproto3"
	"github.com/pkg/errors"
)

// PGTest can be used to send and receive arbitrary pgwire messages on
// Postgres-compatible servers.
type PGTest struct {
	fe   *pgproto3.Frontend
	conn net.Conn
}

// NewPGTest connects to a Postgres server at addr with username user.
func NewPGTest(ctx context.Context, addr, user string) (*PGTest, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()
	fe, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		return nil, errors.Wrap(err, "new frontend")
	}
	if err := fe.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608, // Version 3.0
		Parameters: map[string]string{
			"user": user,
		},
	}); err != nil {
		return nil, errors.Wrap(err, "startup")
	}
	if msg, err := fe.Receive(); err != nil {
		return nil, errors.Wrap(err, "receive")
	} else if auth, ok := msg.(*pgproto3.Authentication); !ok || auth.Type != 0 {
		return nil, errors.Errorf("unexpected: %#v", msg)
	}
	p := &PGTest{
		fe:   fe,
		conn: conn,
	}
	_, err = p.Until(false /* keepErrMsg */, &pgproto3.ReadyForQuery{})
	success = err == nil
	return p, err
}

// Close sends a Terminate message and closes the connection.
func (p *PGTest) Close() error {
	defer p.conn.Close()
	return p.fe.Send(&pgproto3.Terminate{})
}

// Send sends msg to the serrver.
func (p *PGTest) Send(msg pgproto3.FrontendMessage) error {
	if testing.Verbose() {
		fmt.Printf("SEND %T: %+[1]v\n", msg)
	}
	return p.fe.Send(msg)
}

// Receive reads messages until messages of the given types have been found
// in the specified order (with any number of messages in between). It returns
// matched messages.
func (p *PGTest) Receive(
	keepErrMsg bool, typs ...pgproto3.BackendMessage,
) ([]pgproto3.BackendMessage, error) {
	var matched []pgproto3.BackendMessage
	for len(typs) > 0 {
		msgs, err := p.Until(keepErrMsg, typs[0])
		if err != nil {
			return nil, err
		}
		matched = append(matched, msgs[len(msgs)-1])
		typs = typs[1:]
	}
	return matched, nil
}

// Until is like Receive except all messages are returned instead of only
// matched messages.
func (p *PGTest) Until(
	keepErrMsg bool, typs ...pgproto3.BackendMessage,
) ([]pgproto3.BackendMessage, error) {
	var msgs []pgproto3.BackendMessage
	for len(typs) > 0 {
		typ := reflect.TypeOf(typs[0])

		// Receive messages and make copies of them.
		recv, err := p.fe.Receive()
		if err != nil {
			return nil, errors.Wrap(err, "receive")
		}
		if testing.Verbose() {
			fmt.Printf("RECV %T: %+[1]v\n", recv)
		}
		if errmsg, ok := recv.(*pgproto3.ErrorResponse); ok {
			if typ != typErrorResponse {
				return nil, errors.Errorf("waiting for %T, got %#v", typs[0], errmsg)
			}
			var message string
			if keepErrMsg {
				message = errmsg.Message
			}
			// ErrorResponse doesn't encode/decode correctly, so
			// manually append it here.
			msgs = append(msgs, &pgproto3.ErrorResponse{
				Code:    errmsg.Code,
				Message: message,
			})
			typs = typs[1:]
			continue
		}
		// If we saw a ready message but weren't waiting for one, we
		// might wait forever so bail.
		if msg, ok := recv.(*pgproto3.ReadyForQuery); ok && typ != typReadyForQuery {
			return nil, errors.Errorf("waiting for %T, got %#v", typs[0], msg)
		}
		data := recv.Encode(nil)
		// Trim off message type and length.
		data = data[5:]

		x := reflect.New(reflect.ValueOf(recv).Elem().Type())
		msg := x.Interface().(pgproto3.BackendMessage)
		if err := msg.Decode(data); err != nil {
			return nil, errors.Wrap(err, "decode")
		}
		msgs = append(msgs, msg)
		if typ == reflect.TypeOf(msg) {
			typs = typs[1:]
		}
	}
	return msgs, nil
}

var (
	typErrorResponse = reflect.TypeOf(&pgproto3.ErrorResponse{})
	typReadyForQuery = reflect.TypeOf(&pgproto3.ReadyForQuery{})
)
