//
// Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Apache License Version 2.0,
// and you may not use this file except in compliance with the Apache License Version 2.0.
// You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the Apache License Version 2.0 is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
//

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCredentialsProvider(t *testing.T) {
	assert := assert.New(t)

	res, err := GetCredentialsProvider("iam", "am")
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'iam', or neither", err.Error())

	res, err = GetCredentialsProvider("env", "nv")
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())

	res, err = GetCredentialsProvider("iam", "iam")
	assert.NotNil(res)
	assert.Nil(err)

	res, err = GetCredentialsProvider("env", "env")
	assert.NotNil(res)
	assert.Nil(err)

	res, err = GetCredentialsProvider("access", "secret")
	assert.NotNil(res)
	assert.Nil(err)
}

func TestInterfaceToJSONString(t *testing.T) {
	assert := assert.New(t)

	testMap := map[string]string{"string": "hello"}

	assert.Equal(`{"string":"hello"}`, InterfaceToJSONString(testMap, false))
	assert.Equal("{\n  \"string\": \"hello\"\n}", InterfaceToJSONString(testMap, true))
	assert.Equal("{}", InterfaceToJSONString(assert.Equal, true))
}
