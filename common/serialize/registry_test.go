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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSerialization struct {
	name string
}

func (f *fakeSerialization) Name() string                    { return f.name }
func (f *fakeSerialization) Marshal(v any) ([]byte, error)   { return []byte(f.name), nil }
func (f *fakeSerialization) Unmarshal(data []byte, v any) error { return nil }

type fakeDubboSerialization struct {
	fakeSerialization
	id byte
}

func (f *fakeDubboSerialization) ContentTypeID() byte                     { return f.id }
func (f *fakeDubboSerialization) NewObjectOutput(w io.Writer) ObjectOutput { return nil }
func (f *fakeDubboSerialization) NewObjectInput(r io.Reader) ObjectInput   { return nil }

func TestGetSerialization(t *testing.T) {
	s := &fakeSerialization{name: "get-by-name"}
	SetSerialization(s)

	got, err := GetSerialization("get-by-name")
	require.NoError(t, err)
	assert.Same(t, s, got)
}

func TestGetSerializationNotFound(t *testing.T) {
	SetSerialization(&fakeSerialization{name: "listed-name"})

	_, err := GetSerialization("no-such-serialization")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no-such-serialization")
	assert.Contains(t, err.Error(), "listed-name")
}

func TestSetSerializationReplaces(t *testing.T) {
	SetSerialization(&fakeDubboSerialization{fakeSerialization{name: "replaceable"}, 20})
	replacement := &fakeDubboSerialization{fakeSerialization{name: "replaceable"}, 21}
	SetSerialization(replacement)

	got, err := GetSerialization("replaceable")
	require.NoError(t, err)
	assert.Same(t, replacement, got)

	_, err = GetDubboSerializationByID(20)
	assert.Error(t, err, "old id binding must be dropped on replacement")

	byID, err := GetDubboSerializationByID(21)
	require.NoError(t, err)
	assert.Same(t, replacement, byID)
}

func TestSetSerializationReplacementDropsDubboSupport(t *testing.T) {
	SetSerialization(&fakeDubboSerialization{fakeSerialization{name: "downgraded"}, 22})
	SetSerialization(&fakeSerialization{name: "downgraded"})

	got, err := GetSerialization("downgraded")
	require.NoError(t, err)
	_, ok := got.(DubboSerialization)
	assert.False(t, ok, "replacement must no longer implement DubboSerialization")

	_, err = GetDubboSerializationByID(22)
	assert.Error(t, err, "id binding must be dropped when replacement lacks DubboSerialization")
}

func TestGetDubboSerializationByID(t *testing.T) {
	s := &fakeDubboSerialization{fakeSerialization{name: "by-id"}, 23}
	SetSerialization(s)

	got, err := GetDubboSerializationByID(23)
	require.NoError(t, err)
	assert.Same(t, s, got)

	_, err = GetDubboSerializationByID(24)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "24")
}

func TestContentTypeIDConflictFirstWins(t *testing.T) {
	first := &fakeDubboSerialization{fakeSerialization{name: "first"}, 25}
	second := &fakeDubboSerialization{fakeSerialization{name: "second"}, 25}
	SetSerialization(first)
	SetSerialization(second)

	got, err := GetDubboSerializationByID(25)
	require.NoError(t, err)
	assert.Same(t, first, got, "first registration keeps the conflicting id")

	byName, err := GetSerialization("second")
	require.NoError(t, err)
	assert.Same(t, second, byName, "conflicting impl stays reachable by name")
}
