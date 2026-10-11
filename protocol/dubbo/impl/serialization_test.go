/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package impl

import (
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

// hessian2Serialization resolves the built-in hessian2 serialization as a
// DubboSerialization; a convenience for tests exercising the dubbo envelope.
func hessian2Serialization() (serialize.DubboSerialization, error) {
	s, err := serialize.GetSerialization(constant.Hessian2Serialization)
	if err != nil {
		return nil, err
	}
	ss, ok := s.(serialize.DubboSerialization)
	if !ok {
		return nil, fmt.Errorf("serialization %s does not support dubbo protocol", constant.Hessian2Serialization)
	}
	return ss, nil
}

// marshalHessianBody / unmarshalHessianBody encode/decode the hessian
// envelope body with the built-in hessian2 serialization; test convenience
// over encodeBody/decodeBody.
func marshalHessianBody(p DubboPackage) ([]byte, error) {
	ss, err := hessian2Serialization()
	if err != nil {
		return nil, err
	}
	return encodeBody(p, ss)
}

func unmarshalHessianBody(body []byte, p *DubboPackage) error {
	if p.IsHeartBeat() {
		return nil
	}
	ss, err := hessian2Serialization()
	if err != nil {
		return err
	}
	return decodeBody(body, p, ss)
}

// TestLoadSerializerResolvesFromRegistry verifies LoadSerializer resolves
// from the serialization registry by header id, defaulting 0 to hessian2.
func TestLoadSerializerResolvesFromRegistry(t *testing.T) {
	pkg := NewDubboPackage(nil)
	require.NoError(t, LoadSerializer(pkg))
	require.NotNil(t, pkg.Codec.serialization)
	assert.Equal(t, constant.SHessian2, pkg.Codec.serialization.ContentTypeID())
}
