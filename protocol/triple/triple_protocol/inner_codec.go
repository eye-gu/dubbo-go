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

package triple_protocol

import (
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

// resolveInnerCodec resolves the Triple non-IDL inner serialization named by
// serializeType from the global serialization registry
// (serialize.SetSerialization). An empty serializeType defaults to hessian2
// for backward compatibility.
//
// Dubbo Java writes "hessian4" into the wrapper (TripleConstants.HESSIAN4)
// while its on-wire encoding is Hessian2-compatible; the Java receiver maps it
// back to "hessian2" in ReflectionPackableMethod.convertHessianFromWrapper. Go
// mirrors that single alias so a Java non-IDL client is not rejected.
func resolveInnerCodec(serializeType string) (serialize.Serialization, error) {
	if serializeType == "" || serializeType == "hessian4" {
		serializeType = codecNameHessian2
	}
	return serialize.GetSerialization(serializeType)
}
