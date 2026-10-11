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

package msgpack

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

var s Serialization

func TestName(t *testing.T) {
	assert.Equal(t, "msgpack", s.Name())
}

// Wire-format vectors captured from Java MsgpackObjectOutput
// (dubbo-spi-extensions, jackson-dataformat-msgpack 0.9.9).
var (
	javaHelloBytes = "a568656c6c6f" // writeUTF("hello")
	javaByteOne    = "01"           // writeByte(1)
	// writeThrowable(BizException("boom")): the exception class name as a
	// msgpack string, then the exception bean as a map
	javaThrowableClass = "b94d73677061636b436865636b2442697a457863657074696f6e"
	javaThrowableBody  = "85a56361757365c0aa737461636b54726163659188af636c6173734c6f616465724e616d65a3617070aa6d6f64756c654e616d65c0ad6d6f64756c6556657273696f6ec0aa6d6574686f644e616d65a46d61696ea866696c654e616d65b14d73677061636b436865636b2e6a617661aa6c696e654e756d6265721aac6e61746976654d6574686f64c2a9636c6173734e616d65ac4d73677061636b436865636ba76d657373616765a4626f6f6daa7375707072657373656490b06c6f63616c697a65644d657373616765a4626f6f6d"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestDubboSerializationIdentity(t *testing.T) {
	assert.Equal(t, byte(27), s.ContentTypeID())
	var _ serialize.DubboSerialization = s
}

func TestReadJavaWireFormat(t *testing.T) {
	t.Run("utf", func(t *testing.T) {
		in := s.NewObjectInput(bytes.NewReader(mustHex(t, javaHelloBytes)))
		got, err := in.ReadUTF()
		require.NoError(t, err)
		assert.Equal(t, "hello", got)
	})

	t.Run("byte", func(t *testing.T) {
		in := s.NewObjectInput(bytes.NewReader(mustHex(t, javaByteOne)))
		got, err := in.ReadByte()
		require.NoError(t, err)
		assert.Equal(t, byte(1), got)
	})

	t.Run("throwable", func(t *testing.T) {
		stream := append(mustHex(t, javaThrowableClass), mustHex(t, javaThrowableBody)...)
		in := s.NewObjectInput(bytes.NewReader(stream))
		throwable, err := in.ReadThrowable()
		require.NoError(t, err)
		require.Error(t, throwable)
		assert.Contains(t, throwable.Error(), "BizException")
		assert.Contains(t, throwable.Error(), "boom")
	})
}

func TestWriteJavaCompatibleWireFormat(t *testing.T) {
	t.Run("utf", func(t *testing.T) {
		var buf bytes.Buffer
		out := s.NewObjectOutput(&buf)
		require.NoError(t, out.WriteUTF("hello"))
		require.NoError(t, out.Flush())
		assert.Equal(t, mustHex(t, javaHelloBytes), buf.Bytes())
	})

	t.Run("byte", func(t *testing.T) {
		var buf bytes.Buffer
		out := s.NewObjectOutput(&buf)
		require.NoError(t, out.WriteByte(1))
		require.NoError(t, out.Flush())
		assert.Equal(t, mustHex(t, javaByteOne), buf.Bytes())
	})

	t.Run("throwable structure", func(t *testing.T) {
		var buf bytes.Buffer
		out := s.NewObjectOutput(&buf)
		require.NoError(t, out.WriteThrowable(errors.New("boom")))
		require.NoError(t, out.Flush())

		in := s.NewObjectInput(bytes.NewReader(buf.Bytes()))
		className, err := in.ReadUTF()
		require.NoError(t, err)
		assert.Equal(t, "java.lang.RuntimeException", className)

		throwable, err := in.ReadThrowable()
		require.Error(t, err) // stream exhausted after the map
		require.NoError(t, throwable)
	})
}

func TestObjectOutputInputRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	out := s.NewObjectOutput(&buf)
	require.NoError(t, out.WriteUTF("path"))
	require.NoError(t, out.WriteByte(2))
	require.NoError(t, out.WriteObject("arg0"))
	require.NoError(t, out.WriteAttachments(map[string]any{"k": "v"}))
	require.NoError(t, out.Flush())

	in := s.NewObjectInput(bytes.NewReader(buf.Bytes()))
	utf, err := in.ReadUTF()
	require.NoError(t, err)
	assert.Equal(t, "path", utf)

	b, err := in.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte(2), b)

	var arg0 string
	require.NoError(t, in.ReadObject(&arg0))
	assert.Equal(t, "arg0", arg0)

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

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	// ugorji decodes msgpack strings inside interface containers as []byte,
	// so round-trips assert against typed targets.
	t.Run("map", func(t *testing.T) {
		data, err := s.Marshal(map[string]string{"name": "alice", "city": "sz"})
		require.NoError(t, err)
		var got map[string]string
		require.NoError(t, s.Unmarshal(data, &got))
		assert.Equal(t, map[string]string{"name": "alice", "city": "sz"}, got)
	})

	t.Run("slice", func(t *testing.T) {
		data, err := s.Marshal([]string{"a", "b"})
		require.NoError(t, err)
		var got []string
		require.NoError(t, s.Unmarshal(data, &got))
		assert.Equal(t, []string{"a", "b"}, got)
	})

	t.Run("string", func(t *testing.T) {
		data, err := s.Marshal("hello")
		require.NoError(t, err)
		var got string
		require.NoError(t, s.Unmarshal(data, &got))
		assert.Equal(t, "hello", got)
	})
}
