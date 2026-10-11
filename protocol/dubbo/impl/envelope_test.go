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

package impl

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
	"dubbo.apache.org/dubbo-go/v3/remoting"
)

// forwardingSerialization re-exposes the built-in hessian2 envelope under a
// distinct name and id, proving registry dispatch and envelope wire equality.
type forwardingSerialization struct {
	serialize.DubboSerialization
}

func (f forwardingSerialization) Name() string        { return "forwarding-hessian" }
func (f forwardingSerialization) ContentTypeID() byte { return 30 }
func (f forwardingSerialization) NewObjectOutput(w io.Writer) serialize.ObjectOutput {
	return f.DubboSerialization.NewObjectOutput(w)
}
func (f forwardingSerialization) NewObjectInput(r io.Reader) serialize.ObjectInput {
	return f.DubboSerialization.NewObjectInput(r)
}

func newRequestPackage(serialID byte) *DubboPackage {
	return &DubboPackage{
		Header: DubboHeader{Type: PackageRequest_TwoWay, SerialID: serialID, ID: 1},
		Service: Service{
			Path:    "test.Path",
			Version: "1.0.0",
			Method:  "Echo",
			Timeout: time.Second,
		},
		Body:  NewRequestPayload([]any{"hello", int32(7)}, map[string]any{}),
		Codec: NewDubboCodec(nil),
	}
}

// TestCustomSerializationEnvelopeWireCompatible verifies a custom
// DubboSerialization registered via serialize.SetSerialization carries the
// exact same envelope bytes as the built-in hessian2, and that inbound
// decode selects it by header id.
func TestCustomSerializationEnvelopeWireCompatible(t *testing.T) {
	inner, err := hessian2Serialization()
	require.NoError(t, err)
	serialize.SetSerialization(forwardingSerialization{inner})

	custom := newRequestPackage(forwardingSerialization{}.ContentTypeID())
	data, err := custom.Marshal()
	require.NoError(t, err)

	// rewrite the header serialization id to 2 and decode with the built-in
	// hessian2: envelope fields must survive the custom-serialization encode.
	// (byte-level comparison is not stable, hessian map order follows Go map
	// iteration)
	cross := make([]byte, data.Len())
	copy(cross, data.Bytes())
	cross[2] = cross[2]&^SERIAL_MASK | 2
	decoded := NewDubboPackage(bytes.NewBuffer(cross))
	require.NoError(t, decoded.Unmarshal())
	body := decoded.Body.(map[string]any)
	assert.Equal(t, "test.Path", decoded.Service.Path)
	assert.Equal(t, "Echo", decoded.Service.Method)
	assert.Equal(t, []any{"hello", int32(7)}, body["args"])
	attachments := body["attachments"].(map[string]any)
	assert.Equal(t, "test.Path", attachments["path"])
	assert.Equal(t, "1.0.0", attachments["version"])
	assert.Equal(t, "1000", attachments["timeout"])
}

// framedSerialization implements DubboSerialization with an ad-hoc wire
// format (4-byte length-prefixed JSON), proving the envelope code is
// serialization-agnostic end to end.
type framedSerialization struct{}

func (framedSerialization) Name() string        { return "framed" }
func (framedSerialization) ContentTypeID() byte { return 29 }
func (framedSerialization) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
func (framedSerialization) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
func (framedSerialization) NewObjectOutput(w io.Writer) serialize.ObjectOutput {
	return &framedOutput{w: w}
}
func (framedSerialization) NewObjectInput(r io.Reader) serialize.ObjectInput {
	data, err := io.ReadAll(r)
	return &framedInput{data: data, readErr: err}
}

type framedOutput struct {
	w io.Writer
}

func (o *framedOutput) WriteUTF(s string) error { return writeFramed(o.w, []byte(s)) }
func (o *framedOutput) WriteByte(b byte) error {
	_, err := o.w.Write([]byte{b})
	return err
}
func (o *framedOutput) WriteObject(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeFramed(o.w, data)
}
func (o *framedOutput) WriteThrowable(err error) error {
	return o.WriteObject(err.Error())
}
func (o *framedOutput) WriteAttachments(m map[string]any) error {
	return o.WriteObject(m)
}
func (o *framedOutput) Flush() error { return nil }

func writeFramed(w io.Writer, data []byte) error {
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(data)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

type framedInput struct {
	data    []byte
	readErr error
}

func (i *framedInput) readFramed() ([]byte, error) {
	if i.readErr != nil {
		return nil, i.readErr
	}
	if len(i.data) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint32(i.data[:4]))
	i.data = i.data[4:]
	if len(i.data) < n {
		return nil, io.ErrUnexpectedEOF
	}
	val := i.data[:n]
	i.data = i.data[n:]
	return val, nil
}

func (i *framedInput) ReadUTF() (string, error) {
	data, err := i.readFramed()
	return string(data), err
}
func (i *framedInput) ReadByte() (byte, error) {
	if i.readErr != nil {
		return 0, i.readErr
	}
	if len(i.data) < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	b := i.data[0]
	i.data = i.data[1:]
	return b, nil
}
func (i *framedInput) ReadObject(v any) error {
	data, err := i.readFramed()
	if err != nil {
		return err
	}
	if string(data) == "null" {
		return nil
	}
	return json.Unmarshal(data, v)
}
func (i *framedInput) ReadThrowable() (error, error) {
	var msg string
	if err := i.ReadObject(&msg); err != nil {
		return nil, err
	}
	return errors.New(msg), nil
}
func (i *framedInput) ReadAttachments() (map[string]any, error) {
	data, err := i.readFramed()
	if err != nil {
		return nil, err
	}
	if string(data) == "null" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// TestFramedSerializationFullRoundTrip runs the whole dubbo wire path with a
// completely non-hessian serialization: encode request, decode request,
// encode response, decode response.
func TestFramedSerializationFullRoundTrip(t *testing.T) {
	serialize.SetSerialization(framedSerialization{})

	// request encode
	req := newRequestPackage(framedSerialization{}.ContentTypeID())
	reqData, err := req.Marshal()
	require.NoError(t, err)

	// request decode
	decoded := NewDubboPackage(reqData)
	decoded.SetBody(make([]any, 7))
	require.NoError(t, decoded.Unmarshal())
	body := decoded.Body.(map[string]any)
	assert.Equal(t, "test.Path", decoded.Service.Path)
	assert.Equal(t, "1.0.0", decoded.Service.Version)
	assert.Equal(t, "Echo", decoded.Service.Method)
	assert.Equal(t, []any{"hello", float64(7)}, body["args"])
	attachments := body["attachments"].(map[string]any)
	assert.Equal(t, "test.Path", attachments["path"])

	// response encode
	rsp := &DubboPackage{
		Header: DubboHeader{Type: PackageResponse, SerialID: framedSerialization{}.ContentTypeID(), ID: 1, ResponseStatus: Response_OK},
		Body: &ResponsePayload{
			RspObj: "result value",
		},
		Codec: NewDubboCodec(nil),
	}
	rspData, err := rsp.Marshal()
	require.NoError(t, err)

	// response decode; the reply target comes from the pending response
	var reply string
	pending := remoting.NewPendingResponse(1)
	pending.Reply = &reply
	remoting.AddPendingResponse(pending)
	defer remoting.RemovePendingResponse(remoting.SequenceType(1))

	decodedRsp := NewDubboPackage(rspData)
	require.NoError(t, decodedRsp.Unmarshal())
	assert.Equal(t, "result value", reply)
}

// TestMsgpackSerializationDubboWireRoundTrip runs the whole dubbo wire path
// with the built-in msgpack serialization (content type id 27, wire format
// aligned with Java dubbo-spi-extensions MsgpackSerialization).
func TestMsgpackSerializationDubboWireRoundTrip(t *testing.T) {
	msgpackSer, err := serialize.GetSerialization("msgpack")
	require.NoError(t, err)
	msgpackDubbo, ok := msgpackSer.(serialize.DubboSerialization)
	require.True(t, ok)
	require.Equal(t, byte(27), msgpackDubbo.ContentTypeID())

	// request encode + decode
	req := newRequestPackage(27)
	reqData, err := req.Marshal()
	require.NoError(t, err)

	decoded := NewDubboPackage(reqData)
	decoded.SetBody(make([]any, 7))
	require.NoError(t, decoded.Unmarshal())
	body := decoded.Body.(map[string]any)
	assert.Equal(t, "test.Path", decoded.Service.Path)
	assert.Equal(t, "Echo", decoded.Service.Method)
	args := body["args"].([]any)
	assert.Len(t, args, 2)
	assert.EqualValues(t, "hello", args[0])
	assert.EqualValues(t, 7, args[1])
	attachments := body["attachments"].(map[string]any)
	assert.Equal(t, "test.Path", attachments["path"])
	assert.Equal(t, "1000", attachments["timeout"])

	// response encode + decode
	rsp := &DubboPackage{
		Header: DubboHeader{Type: PackageResponse, SerialID: 27, ID: 2, ResponseStatus: Response_OK},
		Body: &ResponsePayload{
			RspObj: "msgpack result",
		},
		Codec: NewDubboCodec(nil),
	}
	rspData, err := rsp.Marshal()
	require.NoError(t, err)

	var reply string
	pending := remoting.NewPendingResponse(2)
	pending.Reply = &reply
	remoting.AddPendingResponse(pending)
	defer remoting.RemovePendingResponse(remoting.SequenceType(2))

	decodedRsp := NewDubboPackage(rspData)
	require.NoError(t, decodedRsp.Unmarshal())
	assert.Equal(t, "msgpack result", reply)
}
