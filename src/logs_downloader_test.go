//
// Copyright (c) 2016-2018 Snowplow Analytics Ltd. All rights reserved.
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
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
)

type mockS3API struct {
	s3iface.S3API
}

func (m *mockS3API) ListObjectsPages(input *s3.ListObjectsInput, fn func(*s3.ListObjectsOutput, bool) bool) error {
	if strings.Contains(*input.Bucket, "error") {
		return errors.New("ListObjectsPages failed")
	}

	files, _ := ioutil.ReadDir(filepath.Join(*input.Bucket, *input.Prefix))
	contents := make([]*s3.Object, len(files))
	sanitizedPrefix := filepath.Join(strings.Split(*input.Prefix, "/")[1:]...)
	for i, file := range files {
		contents[i] = &s3.Object{Key: aws.String(filepath.Join(sanitizedPrefix, file.Name()))}
	}
	fn(&s3.ListObjectsOutput{Contents: contents}, true)
	return nil
}

type mockEMRAPILogs struct {
	emriface.EMRAPI
}

func (m *mockEMRAPILogs) DescribeCluster(input *emr.DescribeClusterInput) (*emr.DescribeClusterOutput, error) {
	if *input.ClusterId == "test-get-bucket" {
		return &emr.DescribeClusterOutput{Cluster: &emr.Cluster{LogUri: aws.String("s3://bucket/log")}},
			nil
	}
	if *input.ClusterId == "test-get-bucket-fail" {
		return &emr.DescribeClusterOutput{Cluster: &emr.Cluster{LogUri: aws.String("://")}}, nil
	}
	if *input.ClusterId == "test-get-bucket-empty-log-uri" {
		return &emr.DescribeClusterOutput{Cluster: &emr.Cluster{LogUri: aws.String("")}}, nil
	}
	if *input.ClusterId == "test-get-step-logs" {
		return &emr.DescribeClusterOutput{Cluster: &emr.Cluster{LogUri: aws.String("s3://tmp-gz/log")}},
			nil
	}
	if *input.ClusterId == "test-get-step-logs-fail" {
		return &emr.DescribeClusterOutput{
			Cluster: &emr.Cluster{LogUri: aws.String("s3://tmp-error/log")},
		}, nil
	}
	return nil, errors.New("DescribeCluster failed")
}

func mockLogsDownloader(jobflowID string) *LogsDownloader {
	return &LogsDownloader{
		JobflowID:  jobflowID,
		EmrSvc:     &mockEMRAPILogs{},
		S3Svc:      &mockS3API{},
		Downloader: &mockDownloaderAPI{},
	}
}
func TestInitLogsDownloader(t *testing.T) {
	assert := assert.New(t)

	ld, err := InitLogsDownloader("env", "env", "eu-west-1", "j-ID")
	assert.NotNil(ld)
	assert.Nil(err)

	ld, err = InitLogsDownloader("env", "nv", "eu-west-1", "j-ID")
	assert.Nil(ld)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())
}

func TestGetStepLogs(t *testing.T) {
	assert := assert.New(t)

	jobflowID := "test-get-step-logs"
	stepID := "step-id"
	tmpDirInput := filepath.Join("tmp-gz", "log", jobflowID, "steps", stepID)
	os.MkdirAll(tmpDirInput, 0755)
	ld := mockLogsDownloader(jobflowID)
	content := filepath.Join(jobflowID, "steps", stepID, "test.gz")
	filename := "test"
	WriteGzFile(filename, tmpDirInput, content)

	contents, err := ld.GetStepLogs(stepID)
	assert.Nil(err)
	assert.NotNil(contents)
	assert.Equal(map[string]string{filename + ".gz": content}, contents)

	os.RemoveAll(tmpDirInput)
}

func TestGetStepLogs_Fail(t *testing.T) {
	assert := assert.New(t)

	stepID := "step-id"
	// fails if GetBucketAndPrefix fails
	ld := mockLogsDownloader("test-get-bucket-fail")
	contents, err := ld.GetStepLogs(stepID)
	assert.Nil(contents)
	assert.NotNil(err)
	assert.Equal("Couldn't parse LogUri: parse ://: missing protocol scheme", err.Error())

	// fails if ListObjectsPages fails
	ld = mockLogsDownloader("test-get-step-logs-fail")
	contents, err = ld.GetStepLogs(stepID)
	assert.Nil(contents)
	assert.NotNil(err)
	assert.Equal("Couldn't download step logs: ListObjectsPages failed", err.Error())
}

func TestDownloadLogFiles(t *testing.T) {
	assert := assert.New(t)
	jobflowID := "download-log-files-jobflow-id"
	ld := mockLogsDownloader(jobflowID)
	prefix := "prefix"
	stepID := "step-id"

	tmpDirInput, _ := ioutil.TempDir("", "input")
	tmpDirOutput, _ := ioutil.TempDir("", "output")
	filepathInput := filepath.Join(tmpDirInput, prefix, jobflowID, "steps", stepID)
	filename := "key.txt"

	os.MkdirAll(filepathInput, 0775)
	ioutil.WriteFile(filepath.Join(filepathInput, filename), []byte("test"), 0644)
	err := ld.DownloadLogFiles(tmpDirInput, prefix, tmpDirOutput, stepID)

	// the mock just writes the file name
	content, err :=
		ioutil.ReadFile(filepath.Join(tmpDirOutput, jobflowID, "steps", stepID, filename))
	assert.Nil(err)
	assert.NotNil(content)
	assert.Equal(filepath.Join(jobflowID, "steps", stepID, filename), string(content[:]))

	os.RemoveAll(tmpDirInput)
	os.RemoveAll(tmpDirOutput)
}

func TestDownloadLogFiles_Fail(t *testing.T) {
	assert := assert.New(t)
	prefix := "prefix"
	stepID := "step-id"

	jobflowID := "download-log-files-jobflow-id"
	ld := mockLogsDownloader(jobflowID)
	// fails if ListObjectsPages fails
	err := ld.DownloadLogFiles("error", prefix, "dir", stepID)
	assert.NotNil(err)
	assert.Equal("ListObjectsPages failed", err.Error())
}

func TestGetBucketAndPrefix(t *testing.T) {
	assert := assert.New(t)

	ld := mockLogsDownloader("test-get-bucket")
	bucket, prefix, err := ld.GetBucketAndPrefix()
	assert.Nil(err)
	assert.Equal("bucket", bucket)
	assert.Equal("log", prefix)
}

func TestGetBucketAndPrefix_Fail(t *testing.T) {
	assert := assert.New(t)

	// fails if DescribeCluster fails
	ld := mockLogsDownloader("error")
	_, _, err := ld.GetBucketAndPrefix()
	assert.NotNil(err)
	assert.Equal("Couldn't fetch LogUri: DescribeCluster failed", err.Error())

	// fails if LogUri is empty
	ld = mockLogsDownloader("test-get-bucket-empty-log-uri")
	_, _, err = ld.GetBucketAndPrefix()
	assert.NotNil(err)
	assert.Equal("LogUri cannot be empty for the logs to be retrieved", err.Error())

	// fails if the LogUri could not be parsed
	ld = mockLogsDownloader("test-get-bucket-fail")
	_, _, err = ld.GetBucketAndPrefix()
	assert.NotNil(err)
	assert.Equal("Couldn't parse LogUri: parse ://: missing protocol scheme", err.Error())
}
