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
	"fmt"
	"io"

	"github.com/ugorji/go/codec"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

// javaRuntimeExceptionClass mirrors the exception class name Java peers can
// resolve via Class.forName when reading a throwable written by Go.
const javaRuntimeExceptionClass = "java.lang.RuntimeException"

// Serialization is the built-in msgpack implementation, serving the triple
// protocol in non-IDL mode and the dubbo wire protocol.
//
// The dubbo wire format mirrors Java's MsgpackSerialization (dubbo-spi-
// extensions, content type id 27): byte values encode as msgpack integers,
// and a throwable is two consecutive values - the exception class name as a
// msgpack string, then the exception fields as a map (jackson encodes
// java.lang.Class as its fully qualified name and Throwable as a bean map).
type Serialization struct{}

func (Serialization) Name() string { return constant.MsgpackSerialization }

// ContentTypeID matches Java Constants.MSGPACK_SERIALIZATION_ID.
func (Serialization) ContentTypeID() byte { return 27 }

func (Serialization) NewObjectOutput(w io.Writer) serialize.ObjectOutput {
	return &objectOutput{encoder: codec.NewEncoder(w, &codec.MsgpackHandle{})}
}

func (Serialization) NewObjectInput(r io.Reader) serialize.ObjectInput {
	return &objectInput{decoder: codec.NewDecoder(r, &codec.MsgpackHandle{})}
}

func (Serialization) Marshal(v any) ([]byte, error) {
	var out []byte
	encoder := codec.NewEncoderBytes(&out, new(codec.MsgpackHandle))
	return out, encoder.Encode(v)
}

func (Serialization) Unmarshal(data []byte, v any) error {
	decoder := codec.NewDecoderBytes(data, new(codec.MsgpackHandle))
	return decoder.Decode(v)
}

func init() {
	serialize.SetSerialization(Serialization{})
}

type objectOutput struct {
	encoder *codec.Encoder
}

func (o *objectOutput) WriteUTF(s string) error               { return o.encoder.Encode(s) }
func (o *objectOutput) WriteByte(b byte) error                { return o.encoder.Encode(int32(b)) }
func (o *objectOutput) WriteObject(v any) error               { return o.encoder.Encode(v) }
func (o *objectOutput) WriteAttachments(m map[string]any) error {
	return o.encoder.Encode(m)
}

func (o *objectOutput) WriteThrowable(err error) error {
	// mirrors MsgpackObjectOutput.writeThrowable: class name, then exception object
	if err == nil {
		return o.encoder.Encode(nil)
	}
	if err := o.encoder.Encode(javaRuntimeExceptionClass); err != nil {
		return err
	}
	return o.encoder.Encode(map[string]any{
		"message":           err.Error(),
		"localizedMessage":  err.Error(),
	})
}

func (o *objectOutput) Flush() error { return nil }

type objectInput struct {
	decoder *codec.Decoder
}

func (i *objectInput) ReadUTF() (string, error) {
	var s string
	if err := i.decoder.Decode(&s); err != nil {
		return "", err
	}
	return s, nil
}

func (i *objectInput) ReadByte() (byte, error) {
	var v int64
	if err := i.decoder.Decode(&v); err != nil {
		return 0, err
	}
	return byte(v), nil
}

func (i *objectInput) ReadObject(v any) error {
	return i.decoder.Decode(v)
}

func (i *objectInput) ReadThrowable() (error, error) {
	// mirrors MsgpackObjectInput.readThrowable: class name, then exception object
	var className string
	if err := i.decoder.Decode(&className); err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := i.decoder.Decode(&fields); err != nil {
		return nil, err
	}
	message := ""
	switch m := fields["message"].(type) {
	case string:
		message = m
	case []byte: // ugorji decodes msgpack strings in interface containers as bytes
		message = string(m)
	}
	return fmt.Errorf("%s: %s", className, message), nil
}

func (i *objectInput) ReadAttachments() (map[string]any, error) {
	var m map[string]any
	if err := i.decoder.Decode(&m); err != nil {
		return nil, err
	}
	for k, v := range m {
		if b, ok := v.([]byte); ok {
			m[k] = string(b)
		}
	}
	return m, nil
}
