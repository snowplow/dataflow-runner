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
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

// S3Downloader models an entity capable of downloading files from S3 at the specified bucket
// to the specified dir
type S3Downloader struct {
	Downloader  s3manageriface.DownloaderAPI
	Bucket, Dir string
}

// EachPage is the function to trigger on each page of s3.ListObjectsPages
func (d *S3Downloader) EachPage(page *s3.ListObjectsOutput, more bool) bool {
	for _, obj := range page.Contents {
		d.DownloadToFile(*obj.Key)
	}

	return true
}

// DownloadToFile downloads the file located at key in S3 to a local file
func (d *S3Downloader) DownloadToFile(key string) error {
	if key == "" {
		return errors.New("Key parameter cannot be empty")
	}

	// Create the directories in the path
	file := filepath.Join(d.Dir, key)
	if err := os.MkdirAll(filepath.Dir(file), 0775); err != nil {
		return err
	}

	// Setup the local file
	fd, err := os.Create(file)
	if err != nil {
		return err
	}
	defer fd.Close()

	// Download the file using the AWS SDK
	params := &s3.GetObjectInput{Bucket: aws.String(d.Bucket), Key: aws.String(key)}
	_, err = d.Downloader.Download(fd, params)
	return err
}
