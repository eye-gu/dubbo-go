/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package triple_protocol

import (
	"encoding/json"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
	"dubbo.apache.org/dubbo-go/v3/protocol/triple/triple_protocol/internal/interoperability"
)

// customJSONSerialization is a non-builtin Serialization proving that
// serializations registered via serialize.SetSerialization serve the triple
// non-IDL path.
type customJSONSerialization struct{}

func (customJSONSerialization) Name() string                       { return "custom-json" }
func (customJSONSerialization) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (customJSONSerialization) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func TestCustomSerializationNonIDLSession(t *testing.T) {
	serialize.SetSerialization(customJSONSerialization{})

	// request side: wrapper decode resolves the inner codec from the registry
	session := &tripleServerCodecSession{delegate: &protoBinaryCodec{}, allowedSerializeType: "custom-json"}
	arg1, _ := json.Marshal("hello")
	arg2, _ := json.Marshal(int64(42))
	wrapper := &interoperability.TripleRequestWrapper{
		SerializeType: "custom-json",
		Args:          [][]byte{arg1, arg2},
		ArgTypes:      []string{"java.lang.String", "long"},
	}
	data, err := proto.Marshal(wrapper)
	require.NoError(t, err)

	results := make([]any, 2)
	for i := range results {
		var v any
		results[i] = &v
	}
	require.NoError(t, session.Unmarshal(data, results))
	assert.Equal(t, "hello", *(results[0].(*any)))
	assert.Equal(t, float64(42), *(results[1].(*any)))
	assert.Equal(t, "custom-json", session.serializeType)

	// response side: the reply wrapper carries the custom serialize type
	respData, err := session.Marshal("result")
	require.NoError(t, err)
	var respWrapper interoperability.TripleResponseWrapper
	require.NoError(t, proto.Unmarshal(respData, &respWrapper))
	assert.Equal(t, "custom-json", respWrapper.SerializeType)
	var got string
	require.NoError(t, json.Unmarshal(respWrapper.Data, &got))
	assert.Equal(t, "result", got)
}

func TestCustomSerializationAllowlistUnchanged(t *testing.T) {
	serialize.SetSerialization(customJSONSerialization{})

	// registered globally, but the provider did not opt in via the
	// serialization parameter: registration alone must not enable it
	session := &tripleServerCodecSession{delegate: &protoBinaryCodec{}, allowedSerializeType: "msgpack"}
	wrapper := &interoperability.TripleRequestWrapper{
		SerializeType: "custom-json",
		Args:          [][]byte{[]byte(`"x"`)},
	}
	data, err := proto.Marshal(wrapper)
	require.NoError(t, err)

	err = session.Unmarshal(data, make([]any, 1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed by provider")
}
