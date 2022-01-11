//
// Copyright (c) 2016-2022 Snowplow Analytics Ltd. All rights reserved.
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
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
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

	res, err = GetCredentialsProvider("default", "faulty")
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'default', or neither", err.Error())

	res, err = GetCredentialsProvider("iam", "iam")
	assert.NotNil(res)
	assert.Nil(err)

	res, err = GetCredentialsProvider("env", "env")
	assert.NotNil(res)
	assert.Nil(err)

	res, err = GetCredentialsProvider("default", "default")
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

func TestStringInSlice(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(true, StringInSlice("a", []string{"a", "b", "c"}))
	assert.Equal(false, StringInSlice("a", []string{"b", "c"}))
}

func TestDiff(t *testing.T) {
	assert := assert.New(t)

	assert.Equal([]string{"a"}, Diff([]string{"b"}, []string{"a", "b"}))
}

func TestReadGzFile(t *testing.T) {
	assert := assert.New(t)
	content := "test"
	filename := WriteGzFile("test-read-gz.txt", "/tmp/", content)

	res, err := ReadGzFile(filename)
	assert.NotNil(res)
	assert.Nil(err)
	assert.Equal(content, res)

	os.Remove(filename)
}

func TestReadGzFile_Fail(t *testing.T) {
	assert := assert.New(t)

	// fails if the file doesn't exist
	res, err := ReadGzFile("/tmp/non-existent-file.gz")
	assert.Equal("", res)
	assert.NotNil(err)
	assert.Equal("open /tmp/non-existent-file.gz: no such file or directory", err.Error())

	// fails if the file is not gz
	filename := "/tmp/test-read-not-gz.txt"
	ioutil.WriteFile(filename, []byte("test"), 06666)
	res, err = ReadGzFile("/tmp/test-read-not-gz.txt")
	assert.Equal("", res)
	assert.NotNil(err)
	assert.Equal("unexpected EOF", err.Error())
	os.Remove(filename)
}

func TestReadGzFiles(t *testing.T) {
	assert := assert.New(t)

	dir, _ := ioutil.TempDir("", "test-read-gzs")
	for i := 0; i < 3; i++ {
		WriteGzFile("test-read-gzs-"+strconv.Itoa(i)+".txt", dir, "test"+strconv.Itoa(i))
	}

	res, err := ReadGzFiles(dir)
	assert.NotNil(res)
	assert.Nil(err)
	assert.Equal(map[string]string{
		"test-read-gzs-0.txt.gz": "test0",
		"test-read-gzs-1.txt.gz": "test1",
		"test-read-gzs-2.txt.gz": "test2",
	}, res)
	os.RemoveAll(dir)
}

func TestReadGzFiles_Fail(t *testing.T) {
	assert := assert.New(t)

	// fails if the dir doesn't exist
	res, err := ReadGzFiles("/not-existing-dir")
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("open /not-existing-dir: no such file or directory", err.Error())

	// fails if it doesn't contain gz
	dir, _ := ioutil.TempDir("", "test-read-gzs-fail")
	ioutil.WriteFile(filepath.Join(dir, "test.gz"), []byte("test"), 06666)
	res, err = ReadGzFiles(dir)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("unexpected EOF", err.Error())
	os.RemoveAll(dir)
}

func WriteGzFile(name, dir, content string) string {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Name = name
	zw.Write([]byte(content))
	zw.Close()
	filename := filepath.Join(dir, zw.Name+".gz")
	ioutil.WriteFile(filename, buf.Bytes(), 0666)
	return filename
}
