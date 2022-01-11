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
	"errors"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/hashicorp/errwrap"
)

// LogsDownloader is used to download failed steps' logs
type LogsDownloader struct {
	JobflowID  string
	EmrSvc     emriface.EMRAPI
	S3Svc      s3iface.S3API
	Downloader s3manageriface.DownloaderAPI
}

// InitLogsDownloader creates a new LogsDownloader instance
func InitLogsDownloader(accessKeyID, secretAccessKey, region, jobflowID string) (*LogsDownloader, error) {
	creds, err := GetCredentialsProvider(accessKeyID, secretAccessKey)
	if err != nil {
		return nil, err
	}

	sess := session.Must(session.NewSession())

	emrSvc := emr.New(sess, &aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	})

	s3Svc := s3.New(sess, &aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	})

	downloader := s3manager.NewDownloaderWithClient(s3Svc)

	return &LogsDownloader{
		JobflowID:  jobflowID,
		EmrSvc:     emrSvc,
		S3Svc:      s3Svc,
		Downloader: downloader,
	}, nil
}

// GetStepLogs retrieves the logs for a particular step from S3 and present them as a map where
// keys are the original file names and values are the contents
func (ld LogsDownloader) GetStepLogs(stepID string) (map[string]string, error) {
	bucket, prefix, err := ld.GetBucketAndPrefix()
	if err != nil {
		return nil, err
	}
	dir, err := ioutil.TempDir("", ld.JobflowID+"-"+stepID)
	if err != nil {
		return nil, errwrap.Wrapf("Couldn't create directory to store the step logs into: {{err}}", err)
	}
	err = ld.DownloadLogFiles(bucket, prefix, dir, stepID)
	if err != nil {
		return nil, errwrap.Wrapf("Couldn't download step logs: {{err}}", err)
	}
	contents, err := ReadGzFiles(filepath.Join(dir, prefix, ld.JobflowID, "steps", stepID))
	if err != nil {
		return nil, errwrap.Wrapf("Coudln't read gzipped log files: {{err}}", err)
	}
	return contents, nil
}

// GetBucketAndPrefix looks for the s3 bucket as well as the prefix where this EMR cluster is
// logging to
func (ld LogsDownloader) GetBucketAndPrefix() (string, string, error) {
	describeClusterInput := &emr.DescribeClusterInput{ClusterId: aws.String(ld.JobflowID)}
	describeClusterOutput, err := ld.EmrSvc.DescribeCluster(describeClusterInput)
	if err != nil {
		return "", "", errwrap.Wrapf("Couldn't fetch LogUri: {{err}}", err)
	}

	rawLogURI := *describeClusterOutput.Cluster.LogUri
	if rawLogURI == "" {
		return "", "", errors.New("LogUri cannot be empty for the logs to be retrieved")
	}
	logURI, err := url.Parse(rawLogURI)
	if err != nil {
		return "", "", errwrap.Wrapf("Couldn't parse LogUri: {{err}}", err)
	}

	return logURI.Host, strings.TrimPrefix(logURI.Path, "/"), nil
}

// DownloadLogFiles takes care of downloading the log files produced by the EMR cluster on S3
// locally to the specified directory
func (ld LogsDownloader) DownloadLogFiles(bucket, prefix, dir, stepID string) error {
	s3Downloader := S3Downloader{Bucket: bucket, Dir: dir, Downloader: ld.Downloader}
	fullPrefix := filepath.Join(prefix, ld.JobflowID, "steps", stepID)
	listObjectsInput := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fullPrefix),
	}
	return ld.S3Svc.ListObjectsPages(listObjectsInput, s3Downloader.EachPage)
}
