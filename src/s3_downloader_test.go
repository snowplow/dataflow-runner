//
// Copyright (c) 2016-2020 Snowplow Analytics Ltd. All rights reserved.
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
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
)

type mockDownloaderAPI struct {
	s3manageriface.DownloaderAPI
}

func (m *mockDownloaderAPI) Download(w io.WriterAt, i *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (int64, error) {
	if *i.Bucket == "" {
		return int64(0), errors.New("Download failed")
	}

	if strings.Contains(*i.Bucket, "gz") {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		zw.Write([]byte(*i.Key))
		zw.Name = *i.Key
		zw.Close()
		_, err := w.WriteAt(buf.Bytes(), int64(0))
		return int64(0), err
	}
	w.WriteAt([]byte(*i.Key), int64(0))
	return int64(0), nil
}

func mockS3Downloader(bucket, dir string) *S3Downloader {
	return &S3Downloader{
		Downloader: &mockDownloaderAPI{},
		Bucket:     bucket,
		Dir:        dir,
	}
}

func TestDownloadToFile(t *testing.T) {
	assert := assert.New(t)
	tmpDir, _ := ioutil.TempDir("", "download-to-file")
	s3Downloader := mockS3Downloader("bucket", tmpDir)

	key := "key"
	err := s3Downloader.DownloadToFile(key)
	assert.Nil(err)

	filepath := filepath.Join(tmpDir, key)

	content, err := ioutil.ReadFile(filepath)
	assert.Nil(err)
	assert.NotNil(content)
	assert.Equal(key, string(content[:]))

	os.RemoveAll(tmpDir)
}

func TestDownloadToFile_Fail(t *testing.T) {
	assert := assert.New(t)

	s3Downloader := mockS3Downloader("bucket", "/tmp2")
	err := s3Downloader.DownloadToFile("")
	assert.NotNil(err)
	assert.Equal("Key parameter cannot be empty", err.Error())

	err = s3Downloader.DownloadToFile("key")
	assert.NotNil(err)
	assert.Equal("mkdir /tmp2: permission denied", err.Error())

	s3Downloader = mockS3Downloader("", "/tmp")
	err = s3Downloader.DownloadToFile("key")
	assert.NotNil(err)
	assert.Equal("Download failed", err.Error())
}

func TestEachPage(t *testing.T) {
	assert := assert.New(t)
	tmpDir, _ := ioutil.TempDir("", "download-to-file")
	s3Downloader := mockS3Downloader("bucket", tmpDir)

	key := "key"
	listObjectsOutput := &s3.ListObjectsOutput{Contents: []*s3.Object{{Key: aws.String(key)}}}
	res := s3Downloader.EachPage(listObjectsOutput, true)
	assert.Equal(true, res)

	filepath := filepath.Join(tmpDir, key)

	content, err := ioutil.ReadFile(filepath)
	assert.Nil(err)
	assert.NotNil(content)
	assert.Equal(key, string(content[:]))

	os.RemoveAll(tmpDir)
}
