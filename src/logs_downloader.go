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
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/snowplow-devops/go-retry"
)

// S3API defines the interface for S3 operations (for mocking in tests)
type S3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3DownloaderAPI defines the interface for S3 download operations (for mocking in tests)
type S3DownloaderAPI interface {
	Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*manager.Downloader)) (n int64, err error)
}

// LogsDownloader is used to download failed steps' logs
type LogsDownloader struct {
	JobflowID  string
	EmrSvc     EMRAPI
	S3Svc      S3API
	Downloader S3DownloaderAPI
}

// InitLogsDownloader creates a new LogsDownloader instance
func InitLogsDownloader(accessKeyID, secretAccessKey, region, jobflowID string) (*LogsDownloader, error) {
	creds, err := GetCredentialsProvider(accessKeyID, secretAccessKey)
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		return nil, err
	}

	emrSvc := emr.NewFromConfig(cfg)
	s3Svc := s3.NewFromConfig(cfg)
	downloader := manager.NewDownloader(s3Svc)

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
	return ld.GetStepLogsWithContext(context.Background(), stepID)
}

// GetStepLogsWithContext retrieves the logs for a particular step with context support
func (ld LogsDownloader) GetStepLogsWithContext(ctx context.Context, stepID string) (map[string]string, error) {
	bucket, prefix, err := ld.GetBucketAndPrefixWithContext(ctx)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", ld.JobflowID+"-"+stepID)
	if err != nil {
		return nil, fmt.Errorf("Couldn't create directory to store the step logs into: %w", err)
	}
	err = ld.DownloadLogFilesWithContext(ctx, bucket, prefix, dir, stepID)
	if err != nil {
		return nil, fmt.Errorf("Couldn't download step logs: %w", err)
	}
	contents, err := ReadGzFiles(filepath.Join(dir, prefix, ld.JobflowID, "steps", stepID))
	if err != nil {
		return nil, fmt.Errorf("Couldn't read gzipped log files: %w", err)
	}
	return contents, nil
}

// GetBucketAndPrefix looks for the s3 bucket as well as the prefix where this EMR cluster is
// logging to
func (ld LogsDownloader) GetBucketAndPrefix() (string, string, error) {
	return ld.GetBucketAndPrefixWithContext(context.Background())
}

// GetBucketAndPrefixWithContext looks for the s3 bucket with context support
func (ld LogsDownloader) GetBucketAndPrefixWithContext(ctx context.Context) (string, string, error) {
	describeClusterInput := &emr.DescribeClusterInput{ClusterId: aws.String(ld.JobflowID)}
	resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.DescribeCluster", func() (any, error) {
		return ld.EmrSvc.DescribeCluster(ctx, describeClusterInput)
	})
	if err != nil {
		return "", "", fmt.Errorf("Couldn't fetch LogUri: %w", err)
	}

	describeClusterOutput := resp.(*emr.DescribeClusterOutput)
	rawLogURI := *describeClusterOutput.Cluster.LogUri
	if rawLogURI == "" {
		return "", "", fmt.Errorf("LogUri cannot be empty for the logs to be retrieved")
	}
	logURI, err := url.Parse(rawLogURI)
	if err != nil {
		return "", "", fmt.Errorf("Couldn't parse LogUri: %w", err)
	}

	return logURI.Host, strings.TrimPrefix(logURI.Path, "/"), nil
}

// DownloadLogFiles takes care of downloading the log files produced by the EMR cluster on S3
// locally to the specified directory
func (ld LogsDownloader) DownloadLogFiles(bucket, prefix, dir, stepID string) error {
	return ld.DownloadLogFilesWithContext(context.Background(), bucket, prefix, dir, stepID)
}

// DownloadLogFilesWithContext downloads log files with context support
func (ld LogsDownloader) DownloadLogFilesWithContext(ctx context.Context, bucket, prefix, dir, stepID string) error {
	s3Downloader := S3Downloader{Bucket: bucket, Dir: dir, Downloader: ld.Downloader}
	fullPrefix := filepath.Join(prefix, ld.JobflowID, "steps", stepID)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fullPrefix),
	}

	// Use SDK v2 paginator for automatic pagination handling
	paginator := s3.NewListObjectsV2Paginator(ld.S3Svc, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, obj := range output.Contents {
			if err := s3Downloader.DownloadToFileWithContext(ctx, aws.ToString(obj.Key)); err != nil {
				return err
			}
		}
	}

	return nil
}
