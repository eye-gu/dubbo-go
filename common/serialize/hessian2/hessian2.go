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
	"errors"
	"fmt"
	"io"
	"reflect"
)

import (
	hessian "github.com/apache/dubbo-go-hessian2"
	"github.com/apache/dubbo-go-hessian2/java_exception"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

// Serialization is the built-in hessian2 implementation, serving both the
// dubbo wire protocol and the triple protocol in non-IDL mode.
type Serialization struct{}

func (Serialization) Name() string          { return constant.Hessian2Serialization }
func (Serialization) ContentTypeID() byte   { return constant.SHessian2 }
func (Serialization) NewObjectOutput(w io.Writer) serialize.ObjectOutput {
	return &objectOutput{encoder: hessian.NewEncoder(), writer: w}
}

func (Serialization) NewObjectInput(r io.Reader) serialize.ObjectInput {
	data, err := io.ReadAll(r)
	return &objectInput{decoder: hessian.NewDecoder(data), readErr: err}
}

func (Serialization) Marshal(v any) ([]byte, error) {
	encoder := hessian.NewEncoder()
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return encoder.Buffer(), nil
}

func (Serialization) Unmarshal(data []byte, v any) error {
	decoder := hessian.NewDecoder(data)
	val, err := decoder.Decode()
	if err != nil {
		return err
	}
	return reflectResponse(val, v)
}

func init() {
	serialize.SetSerialization(Serialization{})
}

type objectOutput struct {
	encoder *hessian.Encoder
	writer  io.Writer
}

func (o *objectOutput) WriteUTF(s string) error  { return o.encode(s, "utf string") }
func (o *objectOutput) WriteByte(b byte) error   { return o.encode(int32(b), "byte") }
func (o *objectOutput) WriteObject(v any) error  { return o.encode(v, "object") }
func (o *objectOutput) WriteAttachments(m map[string]any) error {
	return o.encode(m, "attachments")
}

func (o *objectOutput) WriteThrowable(err error) error {
	var value any
	switch ex := err.(type) {
	case *hessian.GenericException:
		value = java_exception.NewDubboGenericException(ex.ExceptionClass, ex.ExceptionMessage)
	case hessian.GenericException:
		value = java_exception.NewDubboGenericException(ex.ExceptionClass, ex.ExceptionMessage)
	case java_exception.Throwabler:
		value = ex
	default:
		value = java_exception.NewThrowable(err.Error())
	}
	return o.encode(value, "throwable")
}

func (o *objectOutput) Flush() error {
	_, err := o.writer.Write(o.encoder.Buffer())
	return err
}

func (o *objectOutput) encode(v any, description string) error {
	if err := o.encoder.Encode(v); err != nil {
		return fmt.Errorf("failed to encode %s: %w", description, err)
	}
	return nil
}

type objectInput struct {
	decoder *hessian.Decoder
	readErr error
}

func (o *objectInput) ReadUTF() (string, error) {
	v, err := o.decode()
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected utf string, got %T", v)
	}
	return s, nil
}

func (o *objectInput) ReadByte() (byte, error) {
	v, err := o.decode()
	if err != nil {
		return 0, err
	}
	i, ok := v.(int32)
	if !ok {
		return 0, fmt.Errorf("expected byte, got %T", v)
	}
	return byte(i), nil
}

func (o *objectInput) ReadObject(v any) error {
	val, err := o.decode()
	if err != nil {
		return err
	}
	return reflectResponse(val, v)
}

func (o *objectInput) ReadThrowable() (error, error) {
	v, err := o.decode()
	if err != nil {
		return nil, err
	}
	if g, ok := hessian.ToGenericException(v); ok {
		return g, nil
	}
	if e, ok := v.(error); ok {
		return e, nil
	}
	return fmt.Errorf("got exception: %+v", v), nil
}

// ReadAttachments returns a nil map (without error) when the decoded value is
// absent (nil or empty string); whether absence is legal is protocol policy.
func (o *objectInput) ReadAttachments() (map[string]any, error) {
	v, err := o.decode()
	if err != nil {
		return nil, err
	}
	if v == nil || v == "" {
		return nil, nil
	}
	m, ok := v.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("get wrong attachments: %+v", v)
	}
	return toMapStringInterface(m), nil
}

func (o *objectInput) decode() (any, error) {
	if o.readErr != nil {
		return nil, o.readErr
	}
	return o.decoder.Decode()
}

func reflectResponse(in any, out any) error {
	if in == nil {
		return fmt.Errorf("@in is nil")
	}

	if out == nil {
		return fmt.Errorf("@out is nil")
	}
	if reflect.TypeOf(out).Kind() != reflect.Pointer {
		return errors.New("@out should be a pointer")
	}

	inValue := hessian.EnsurePackValue(in)
	outValue := hessian.EnsurePackValue(out)

	outType := outValue.Type().String()
	if outType == "interface {}" || outType == "*interface {}" {
		hessian.SetValue(outValue, inValue)
		return nil
	}

	switch inValue.Type().Kind() {
	case reflect.Slice, reflect.Array:
		return copySlice(inValue, outValue)
	case reflect.Map:
		return copyMap(inValue, outValue)
	default:
		hessian.SetValue(outValue, inValue)
	}

	return nil
}

// copySlice copy from inSlice to outSlice
func copySlice(inSlice, outSlice reflect.Value) error {
	if inSlice.IsNil() {
		return errors.New("@in is nil")
	}
	if inSlice.Kind() != reflect.Slice {
		return fmt.Errorf("@in is not slice, but %v", inSlice.Kind())
	}

	for outSlice.Kind() == reflect.Pointer {
		outSlice = outSlice.Elem()
	}

	size := inSlice.Len()
	outSlice.Set(reflect.MakeSlice(outSlice.Type(), size, size))

	for i := range size {
		inSliceValue := inSlice.Index(i)
		if !inSliceValue.Type().AssignableTo(outSlice.Index(i).Type()) {
			return fmt.Errorf("in element type [%s] can not assign to out element type [%s]",
				inSliceValue.Type().String(), outSlice.Type().String())
		}
		outSlice.Index(i).Set(inSliceValue)
	}

	return nil
}

// copyMap copy from in map to out map
func copyMap(inMapValue, outMapValue reflect.Value) error {
	if inMapValue.IsNil() {
		return errors.New("@in is nil")
	}
	if !inMapValue.CanInterface() {
		return errors.New("@in's Interface can not be used")
	}
	if inMapValue.Kind() != reflect.Map {
		return fmt.Errorf("@in is not map, but %v", inMapValue.Kind())
	}

	outMapType := hessian.UnpackPtrType(outMapValue.Type())
	hessian.SetValue(outMapValue, reflect.MakeMap(outMapType))

	outKeyType := outMapType.Key()

	outMapValue = hessian.UnpackPtrValue(outMapValue)
	outValueType := outMapValue.Type().Elem()

	for _, inKey := range inMapValue.MapKeys() {
		inValue := inMapValue.MapIndex(inKey)

		if !inKey.Type().AssignableTo(outKeyType) {
			return fmt.Errorf("in Key:{type:%s, value:%#v} can not assign to out Key:{type:%s} ",
				inKey.Type().String(), inKey, outKeyType.String())
		}
		if !inValue.Type().AssignableTo(outValueType) {
			return fmt.Errorf("in Value:{type:%s, value:%#v} can not assign to out value:{type:%s}",
				inValue.Type().String(), inValue, outValueType.String())
		}
		outMapValue.SetMapIndex(inKey, inValue)
	}

	return nil
}

func toMapStringInterface(origin map[any]any) map[string]any {
	dest := make(map[string]any, len(origin))
	for k, v := range origin {
		if kv, ok := k.(string); ok {
			if v == nil {
				dest[kv] = ""
				continue
			}
			dest[kv] = v
		}
	}
	return dest
}
