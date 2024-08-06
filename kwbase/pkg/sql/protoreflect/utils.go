// Copyright 2020 The Cockroach Authors.
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

package protoreflect

import (
	"reflect"
	"strings"

	jsonb "gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

// NewMessage creates a new protocol message object, given its fully
// qualified name.
func NewMessage(name string) (protoutil.Message, error) {
	// Get the reflected type of the protocol message.
	rt := proto.MessageType(name)
	if rt == nil {
		return nil, errors.Newf("unknown proto message type %s", name)
	}

	// If the message is known, we should get the pointer to our message.
	if rt.Kind() != reflect.Ptr {
		return nil, errors.AssertionFailedf(
			"expected ptr to message, got %s instead", rt.Kind().String())
	}

	// Construct message of appropriate type, through reflection.
	rv := reflect.New(rt.Elem())
	msg, ok := rv.Interface().(protoutil.Message)

	if !ok {
		// Just to be safe;
		return nil, errors.AssertionFailedf(
			"unexpected proto type for %s; expected protoutil.Message, got %T",
			name, rv.Interface())
	}
	return msg, nil
}

// DecodeMessage decodes protocol message specified as its fully
// qualified name, and it's marshaled data, into a protoutil.Message.
func DecodeMessage(name string, data []byte) (protoutil.Message, error) {
	msg, err := NewMessage(name)
	if err != nil {
		return nil, err
	}

	// Now, parse data as our proto message.
	if err := protoutil.Unmarshal(data, msg); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal proto %s", name)
	}
	return msg, nil
}

// MessageToJSON converts a protocol message into a JSONB object. The
// emitDefaults flag dictates whether fields with zero values are rendered or
// not.
func MessageToJSON(msg protoutil.Message, emitDefaults bool) (jsonb.JSON, error) {
	// Convert to json.
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: emitDefaults}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling %s; msg=%+v", proto.MessageName(msg), msg)
	}

	return jsonb.ParseJSON(msgJSON)
}

// JSONBMarshalToMessage initializes the target protocol message with
// the data in the input JSONB object.
// Returns serialized byte representation of the protocol message.
func JSONBMarshalToMessage(input jsonb.JSON, target protoutil.Message) ([]byte, error) {
	json := &jsonpb.Unmarshaler{}
	if err := json.Unmarshal(strings.NewReader(input.String()), target); err != nil {
		return nil, errors.Wrapf(err, "unmarshaling json to %s", proto.MessageName(target))
	}
	data, err := protoutil.Marshal(target)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling to proto %s", proto.MessageName(target))
	}
	return data, nil
}
