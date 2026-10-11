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

package serialize

import (
	"io"
)

// Serialization is the object-level serialization contract, consumed by the
// triple protocol in non-IDL mode.
//
// Name is the interoperability identity: triple writes it into
// TripleRequestWrapper/TripleResponseWrapper.SerializeType on the wire, so it
// must stay aligned with the naming scheme of Java peers (e.g. "hessian2",
// "msgpack").
type Serialization interface {
	Name() string
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// DubboSerialization is the extra contract required by the dubbo wire
// protocol on top of Serialization. A Serialization that also implements
// DubboSerialization can be selected for the dubbo protocol.
//
// ContentTypeID occupies the 5-bit serialization id field of the dubbo
// protocol header: it must not exceed 31 and must not collide with the ids
// reserved by Java dubbo (org.apache.dubbo.common.serialize.Constants).
type DubboSerialization interface {
	Serialization
	ContentTypeID() byte
	NewObjectOutput(w io.Writer) ObjectOutput
	NewObjectInput(r io.Reader) ObjectInput
}

// ObjectOutput writes dubbo protocol envelope fields. One instance is bound
// to a single body stream; implementations may carry per-stream state (e.g.
// hessian reference tables), so instances are not reusable. Flush terminates
// the stream and writes buffered bytes to the underlying writer.
type ObjectOutput interface {
	WriteUTF(string) error
	WriteByte(byte) error
	WriteObject(any) error
	WriteThrowable(error) error
	WriteAttachments(map[string]any) error
	Flush() error
}

// ObjectInput reads dubbo protocol envelope fields. ReadObject decodes into
// the pointer target v, with the expected type supplied by the codec.
// ReadThrowable returns the decoded throwable as an error; a non-nil second
// return reports a stream read failure.
type ObjectInput interface {
	ReadUTF() (string, error)
	ReadByte() (byte, error)
	ReadObject(v any) error
	ReadThrowable() (throwable error, err error)
	ReadAttachments() (map[string]any, error)
}
