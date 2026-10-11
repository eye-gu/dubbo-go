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
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

import (
	hessian "github.com/apache/dubbo-go-hessian2"

	"github.com/dubbogo/gost/log/logger"

	perrors "github.com/pkg/errors"
)

import (
	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

// encodeBody writes the dubbo wire protocol body (request or response
// envelope) with the primitives of the given serialization.
func encodeBody(p DubboPackage, ss serialize.DubboSerialization) ([]byte, error) {
	var buf bytes.Buffer
	out := ss.NewObjectOutput(&buf)

	if p.IsHeartBeat() && p.Header.ResponseStatus == Zero {
		// heartbeat request body carries a single null token
		if err := out.WriteObject(nil); err != nil {
			return nil, err
		}
	} else if p.IsRequest() {
		if err := encodeRequestBody(out, p); err != nil {
			return nil, err
		}
	} else {
		if err := encodeResponseBody(out, p); err != nil {
			return nil, err
		}
		// response bodies are terminated by a trailing null, dubbo protocol compat
		if err := out.WriteObject(nil); err != nil {
			return nil, err
		}
	}

	if err := out.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeBody reads the dubbo wire protocol body (request or response
// envelope) with the primitives of the given serialization.
func decodeBody(body []byte, p *DubboPackage, ss serialize.DubboSerialization) error {
	in := ss.NewObjectInput(bytes.NewReader(body))
	if p.IsRequest() {
		return decodeRequestBody(in, p)
	}
	return decodeResponseBody(in, p)
}

func encodeRequestBody(out serialize.ObjectOutput, p DubboPackage) error {
	service := p.Service
	request := EnsureRequestPayload(p.Body)
	if err := out.WriteUTF(DEFAULT_DUBBO_PROTOCOL_VERSION); err != nil {
		return err
	}
	if err := out.WriteUTF(service.Path); err != nil {
		return err
	}
	if err := out.WriteUTF(service.Version); err != nil {
		return err
	}
	if err := out.WriteUTF(service.Method); err != nil {
		return err
	}

	args, ok := request.Params.([]any)

	if !ok {
		logger.Infof("[Dubbo] request args, params=%v", request.Params)
		return fmt.Errorf("@params is not of type: []any")
	}
	types, err := GetArgsTypeList(args)
	if err != nil {
		return fmt.Errorf(" PackRequest(args:%+v): %w", args, err)
	}
	if err := out.WriteUTF(types); err != nil {
		return err
	}
	for _, v := range args {
		if err := out.WriteObject(v); err != nil {
			return fmt.Errorf("failed to encode argument: %v: %w", v, err)
		}
	}

	request.Attachments[PATH_KEY] = service.Path
	request.Attachments[VERSION_KEY] = service.Version
	if len(service.Group) > 0 {
		request.Attachments[GROUP_KEY] = service.Group
	}
	if len(service.Interface) > 0 {
		request.Attachments[INTERFACE_KEY] = service.Interface
	}
	if service.Timeout != 0 {
		request.Attachments[TIMEOUT_KEY] = strconv.Itoa(int(service.Timeout / time.Millisecond))
	}

	return out.WriteAttachments(request.Attachments)
}

func encodeResponseBody(out serialize.ObjectOutput, p DubboPackage) error {
	response := EnsureResponsePayload(p.Body)
	if p.Header.ResponseStatus != Response_OK {
		if response.Exception != nil {
			return out.WriteObject(response.Exception.Error())
		}
		return out.WriteObject(response.RspObj)
	}
	if p.IsHeartBeat() {
		return out.WriteObject(nil)
	}

	var version string
	if attachmentVersion, ok := response.Attachments[DUBBO_VERSION_KEY]; ok {
		version = attachmentVersion.(string)
	}
	withAttachments := isSupportResponseAttachment(version)
	resWithException, resValue, resNullValue := responseTypes(withAttachments)

	if response.Exception != nil {
		if err := out.WriteByte(byte(resWithException)); err != nil {
			return err
		}
		if err := out.WriteThrowable(response.Exception); err != nil {
			return err
		}
	} else if response.RspObj == nil {
		if err := out.WriteByte(byte(resNullValue)); err != nil {
			return err
		}
	} else {
		if err := out.WriteByte(byte(resValue)); err != nil {
			return err
		}
		if err := out.WriteObject(response.RspObj); err != nil {
			return err
		}
	}
	if withAttachments {
		return out.WriteAttachments(response.Attachments)
	}
	return nil
}

func responseTypes(withAttachments bool) (int32, int32, int32) {
	if withAttachments {
		return RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS, RESPONSE_VALUE_WITH_ATTACHMENTS, RESPONSE_NULL_VALUE_WITH_ATTACHMENTS
	}
	return RESPONSE_WITH_EXCEPTION, RESPONSE_VALUE, RESPONSE_NULL_VALUE
}

// https://github.com/apache/dubbo/blob/dubbo-2.7.1/dubbo-common/src/main/java/org/apache/dubbo/common/Version.java#L96
// isSupportResponseAttachment is for compatibility among some dubbo version
func isSupportResponseAttachment(version string) bool {
	if version == "" {
		return false
	}

	v := version2Int(version)
	if v == -1 {
		return false
	}

	if v >= 2001000 && v <= 2060200 { // 2.0.10 ~ 2.6.2
		return false
	}
	return v >= LOWEST_VERSION_FOR_RESPONSE_ATTACHMENT
}

func version2Int(version string) int {
	v := 0
	varr := strings.Split(version, ".")
	length := len(varr)
	for key, value := range varr {
		v0, err := strconv.Atoi(value)
		if err != nil {
			return -1
		}
		v += v0 * int(math.Pow10((length-key-1)*2))
	}
	if length == 3 {
		return v * 100
	}
	return v
}

func decodeRequestBody(in serialize.ObjectInput, p *DubboPackage) error {
	if p.Body == nil {
		p.SetBody(make([]any, 7))
	}
	req, ok := p.Body.([]any)
	if !ok {
		return fmt.Errorf("@reqObj is not of type: []any")
	}

	dubboVersion, err := in.ReadUTF()
	if err != nil {
		return perrors.WithStack(err)
	}
	req[0] = dubboVersion

	target, err := in.ReadUTF()
	if err != nil {
		return perrors.WithStack(err)
	}
	req[1] = target

	serviceVersion, err := in.ReadUTF()
	if err != nil {
		return perrors.WithStack(err)
	}
	req[2] = serviceVersion

	method, err := in.ReadUTF()
	if err != nil {
		return perrors.WithStack(err)
	}
	req[3] = method

	argsTypes, err := in.ReadUTF()
	if err != nil {
		return perrors.WithStack(err)
	}
	req[4] = argsTypes

	ats := hessian.DescRegex.FindAllString(argsTypes, -1)
	var args []any
	for range ats {
		var arg any
		if err := in.ReadObject(&arg); err != nil {
			return perrors.WithStack(err)
		}
		args = append(args, arg)
	}
	req[5] = args

	attachments, err := in.ReadAttachments()
	if err != nil {
		return perrors.WithStack(err)
	}
	if attachments == nil {
		attachments = map[string]any{constant.InterfaceKey: target}
	}
	attachments[DUBBO_VERSION_KEY] = dubboVersion
	req[6] = attachments

	buildServerSidePackageBody(p)
	return nil
}

func decodeResponseBody(in serialize.ObjectInput, p *DubboPackage) error {
	if p.Body == nil {
		p.SetBody(&ResponsePayload{})
	}
	rspType, err := in.ReadByte()
	if err != nil {
		return perrors.WithStack(err)
	}
	response := EnsureResponsePayload(p.Body)

	switch rspType {
	case byte(RESPONSE_WITH_EXCEPTION), byte(RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS):
		exception, err := in.ReadThrowable()
		if err != nil {
			return perrors.WithStack(err)
		}
		if rspType == byte(RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS) {
			attachments, err := in.ReadAttachments()
			if err != nil {
				return perrors.WithStack(err)
			}
			if attachments == nil {
				return fmt.Errorf("get wrong attachments: %+v", attachments)
			}
			response.Attachments = attachments
		}
		response.Exception = exception
		return nil

	case byte(RESPONSE_VALUE), byte(RESPONSE_VALUE_WITH_ATTACHMENTS):
		if err := in.ReadObject(response.RspObj); err != nil {
			return perrors.WithStack(err)
		}
		if rspType == byte(RESPONSE_VALUE_WITH_ATTACHMENTS) {
			attachments, err := in.ReadAttachments()
			if err != nil {
				return perrors.WithStack(err)
			}
			if attachments == nil {
				return fmt.Errorf("get wrong attachments: %+v", attachments)
			}
			response.Attachments = attachments
		}
		return nil

	case byte(RESPONSE_NULL_VALUE), byte(RESPONSE_NULL_VALUE_WITH_ATTACHMENTS):
		if rspType == byte(RESPONSE_NULL_VALUE_WITH_ATTACHMENTS) {
			attachments, err := in.ReadAttachments()
			if err != nil {
				return perrors.WithStack(err)
			}
			if attachments == nil {
				return fmt.Errorf("get wrong attachments: %+v", attachments)
			}
			response.Attachments = attachments
		}
		return nil
	}
	return nil
}

func buildServerSidePackageBody(pkg *DubboPackage) {
	req := pkg.GetBody().([]any) // length of body should be 7
	if len(req) > 0 {
		var dubboVersion, argsTypes string
		var args []any
		var attachments map[string]any
		svc := Service{}
		if req[0] != nil {
			dubboVersion = req[0].(string)
		}
		if req[1] != nil {
			svc.Path = req[1].(string)
		}
		if req[2] != nil {
			svc.Version = req[2].(string)
		}
		if req[3] != nil {
			svc.Method = req[3].(string)
		}
		if req[4] != nil {
			argsTypes = req[4].(string)
		}
		if req[5] != nil {
			args = req[5].([]any)
		}
		if req[6] != nil {
			attachments = req[6].(map[string]any)
		}
		if svc.Path == "" && attachments[constant.PathKey] != nil && len(attachments[constant.PathKey].(string)) > 0 {
			svc.Path = attachments[constant.PathKey].(string)
		}
		if _, ok := attachments[constant.InterfaceKey]; ok {
			svc.Interface = attachments[constant.InterfaceKey].(string)
		} else {
			svc.Interface = svc.Path
		}
		if _, ok := attachments[constant.GroupKey]; ok {
			svc.Group = attachments[constant.GroupKey].(string)
		}
		pkg.SetService(svc)
		pkg.SetBody(map[string]any{
			"dubboVersion": dubboVersion,
			"argsTypes":    argsTypes,
			"args":         args,
			"service":      common.ServiceMap.GetService(DUBBO, svc.Interface, svc.Group, svc.Version), // path as a key
			"attachments":  attachments,
		})
	}
}

func GetArgsTypeList(args []any) (string, error) {
	var (
		typ   string
		types string
	)

	for i := range args {
		typ = getArgType(args[i])
		if typ == "" {
			return types, fmt.Errorf("cat not get arg %#v type", args[i])
		}
		if !strings.Contains(typ, ".") {
			types += typ
		} else if strings.Index(typ, "[") == 0 {
			types += strings.ReplaceAll(typ, ".", "/")
		} else {
			// java.util.List -> Ljava/util/List;
			types += "L" + strings.ReplaceAll(typ, ".", "/") + ";"
		}
	}

	return types, nil
}

func getArgType(v any) string {
	if v == nil {
		return "V"
	}

	switch v := v.(type) {
	// Serialized tags for base types=
	case bool:
		return "Z"
	case []bool:
		return "[Z"
	case byte:
		return "B"
	case []byte:
		return "[B"
	case int8:
		return "B"
	case []int8:
		return "[B"
	case int16:
		return "S"
	case []int16:
		return "[S"
	case uint16: // Equivalent to Char of Java
		return "C"
	case []uint16:
		return "[C"
	case int:
		return "J"
	case []int:
		return "[J"
	case int32:
		return "I"
	case []int32:
		return "[I"
	case int64:
		return "J"
	case []int64:
		return "[J"
	case time.Time:
		return "java.util.Date"
	case []time.Time:
		return "[Ljava.util.Date"
	case float32:
		return "F"
	case []float32:
		return "[F"
	case float64:
		return "D"
	case []float64:
		return "[D"
	case string:
		return "java.lang.String"
	case []string:
		return "[Ljava.lang.String;"
	case []hessian.Object:
		return "[Ljava.lang.Object;"
	case map[any]any:
		// return  "java.util.HashMap"
		return "java.util.Map"
	case hessian.POJOEnum:
		return v.JavaClassName()
	case *int8:
		return "java.lang.Byte"
	case *int16:
		return "java.lang.Short"
	case *uint16:
		return "java.lang.Character"
	case *int:
		return "java.lang.Long"
	case *int32:
		return "java.lang.Integer"
	case *int64:
		return "java.lang.Long"
	case *float32:
		return "java.lang.Float"
	case *float64:
		return "java.lang.Double"
	//  Serialized tags for complex types
	default:
		t := reflect.TypeOf(v)
		if reflect.Pointer == t.Kind() {
			t = reflect.TypeOf(reflect.ValueOf(v).Elem())
		}
		switch t.Kind() {
		case reflect.Struct:
			p, ok := v.(hessian.Param)
			if ok {
				return p.JavaParamName()
			}
			v, ok := v.(hessian.POJO)
			if ok {
				return v.JavaClassName()
			}
			return "java.lang.Object"
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Struct {
				return "[Ljava.lang.Object;"
			}
			// return "java.util.ArrayList"
			return "java.util.List"
		case reflect.Map: // Enter here, map may be map[string]int
			return "java.util.Map"
		default:
			return ""
		}
	}

	// unreachable
	// return "java.lang.RuntimeException"
}

func ToMapStringInterface(origin map[any]any) map[string]any {
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
