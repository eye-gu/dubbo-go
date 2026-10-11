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

package hessian2

import (
	"bytes"
	"errors"
	"testing"
)

import (
	hessian "github.com/apache/dubbo-go-hessian2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

type testUser struct {
	Name string
	Age  int
}

func (u *testUser) JavaClassName() string { return "test.User" }

func init() {
	hessian.RegisterPOJO(&testUser{})
}

var s Serialization

func TestSerializationIdentity(t *testing.T) {
	assert.Equal(t, "hessian2", s.Name())
	assert.Equal(t, byte(2), s.ContentTypeID())
	var _ serialize.DubboSerialization = s
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	cases := []any{
		"hello",
		int32(42),
		[]any{"a", int32(1)},
		&testUser{Name: "alice", Age: 20},
	}
	for _, v := range cases {
		data, err := s.Marshal(v)
		require.NoError(t, err)

		target := reflectNew(v)
		require.NoError(t, s.Unmarshal(data, target))
		assert.Equal(t, v, indirect(target))
	}
}

func TestObjectOutputInputRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	out := s.NewObjectOutput(&buf)
	require.NoError(t, out.WriteUTF("path"))
	require.NoError(t, out.WriteByte(1))
	require.NoError(t, out.WriteObject("arg0"))
	require.NoError(t, out.WriteObject(&testUser{Name: "bob", Age: 30}))
	require.NoError(t, out.WriteAttachments(map[string]any{"k": "v"}))
	require.NoError(t, out.Flush())

	in := s.NewObjectInput(bytes.NewReader(buf.Bytes()))
	utf, err := in.ReadUTF()
	require.NoError(t, err)
	assert.Equal(t, "path", utf)

	b, err := in.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte(1), b)

	var arg0 string
	require.NoError(t, in.ReadObject(&arg0))
	assert.Equal(t, "arg0", arg0)

	var user testUser
	require.NoError(t, in.ReadObject(&user))
	assert.Equal(t, testUser{Name: "bob", Age: 30}, user)

	attachments, err := in.ReadAttachments()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"k": "v"}, attachments)
}

func TestThrowableRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	out := s.NewObjectOutput(&buf)
	require.NoError(t, out.WriteThrowable(errors.New("boom")))
	require.NoError(t, out.Flush())

	in := s.NewObjectInput(bytes.NewReader(buf.Bytes()))
	throwable, err := in.ReadThrowable()
	require.NoError(t, err)
	require.Error(t, throwable)
	assert.Contains(t, throwable.Error(), "boom")
}

func TestReadAttachmentsAbsent(t *testing.T) {
	var buf bytes.Buffer
	out := s.NewObjectOutput(&buf)
	require.NoError(t, out.WriteObject(nil))
	require.NoError(t, out.Flush())

	in := s.NewObjectInput(bytes.NewReader(buf.Bytes()))
	attachments, err := in.ReadAttachments()
	require.NoError(t, err)
	assert.Nil(t, attachments)
}

func reflectNew(v any) any {
	switch v.(type) {
	case string:
		return new(string)
	case int32:
		return new(int32)
	case []any:
		return new([]any)
	case *testUser:
		return new(testUser)
	}
	panic("unsupported test target")
}

func indirect(v any) any {
	switch t := v.(type) {
	case *string:
		return *t
	case *int32:
		return *t
	case *[]any:
		return *t
	case *testUser:
		return t
	}
	panic("unsupported test target")
}
