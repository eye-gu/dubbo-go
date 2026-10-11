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
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/serialize"
)

// registers the built-in serializations whenever this package is used
import (
	_ "dubbo.apache.org/dubbo-go/v3/common/serialize/hessian2"
	_ "dubbo.apache.org/dubbo-go/v3/common/serialize/msgpack"
)

// LoadSerializer resolves and pins the serialization of the package by its
// header serialization id (defaulting to hessian2, as before).
//
// Deprecated: the codec resolves serializations itself; pin one explicitly
// via SetSerialization if needed.
func LoadSerializer(p *DubboPackage) error {
	// NOTE: default serialID is S_Hessian
	serialID := p.Header.SerialID
	if serialID == 0 {
		serialID = constant.SHessian2
	}
	ss, err := serialize.GetDubboSerializationByID(serialID)
	if err != nil {
		return err
	}
	p.SetSerialization(ss)
	return nil
}
