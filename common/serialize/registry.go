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
	"fmt"
	"maps"
	"slices"
	"sync"
)

import (
	"github.com/dubbogo/gost/log/logger"
)

var (
	mu             sync.RWMutex
	serializations = make(map[string]Serialization)
	idToName       = make(map[byte]string)
)

// SetSerialization registers s under s.Name(), replacing any previous
// registration of the same name. When s also implements DubboSerialization,
// its content type id is indexed for GetDubboSerializationByID; a conflicting
// id keeps the first registration and logs an error.
func SetSerialization(s Serialization) {
	name := s.Name()
	mu.Lock()
	defer mu.Unlock()
	// drop the id binding of the impl being replaced
	for id, n := range idToName {
		if n == name {
			delete(idToName, id)
		}
	}
	serializations[name] = s
	ds, ok := s.(DubboSerialization)
	if !ok {
		return
	}
	id := ds.ContentTypeID()
	if holder, taken := idToName[id]; taken {
		logger.Errorf("serialization %s: content type id %d conflicts with %s, keeping %s", name, id, holder, holder)
		return
	}
	idToName[id] = name
}

// GetSerialization returns the Serialization registered under name.
func GetSerialization(name string) (Serialization, error) {
	mu.RLock()
	defer mu.RUnlock()
	if s, ok := serializations[name]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("serialization %q not found, registered: %v", name, registeredNames())
}

// GetDubboSerializationByID resolves a dubbo protocol serialization by the
// serialization id byte carried in the protocol header.
func GetDubboSerializationByID(id byte) (DubboSerialization, error) {
	mu.RLock()
	defer mu.RUnlock()
	name, ok := idToName[id]
	if !ok {
		return nil, fmt.Errorf("serialization id %d not found, registered ids: %v", id, registeredIDs())
	}
	ds, ok := serializations[name].(DubboSerialization)
	if !ok {
		return nil, fmt.Errorf("serialization %s does not support dubbo protocol", name)
	}
	return ds, nil
}

// caller must hold mu (read or write)
func registeredNames() []string {
	names := slices.Collect(maps.Keys(serializations))
	slices.Sort(names)
	return names
}

// caller must hold mu (read or write)
func registeredIDs() []byte {
	ids := make([]byte, 0, len(idToName))
	for id := range idToName {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
