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
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
)

// GetCredentialsProvider attempts to fetch credentials from either:
// 1. IAM Role (EC2 instance role via IMDS)
// 2. ENV Variables (uses default credential chain which handles env vars)
// 3. Default Credential Chain
// 4. Static Credentials
//
// All credential providers are wrapped with aws.NewCredentialsCache for:
// - Thread-safe concurrent access
// - Automatic credential caching and refresh
func GetCredentialsProvider(a string, s string) (aws.CredentialsProvider, error) {
	if isIam(a) && isIam(s) {
		// EC2 role credentials must be wrapped for thread safety and caching
		return aws.NewCredentialsCache(ec2rolecreds.New()), nil
	} else if isEnv(a) && isEnv(s) {
		// Use default credential chain which properly handles environment variables
		// and supports credential refresh
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		return cfg.Credentials, nil
	} else if isDefault(a) && isDefault(s) {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		return cfg.Credentials, nil
	} else if isIam(a) || isIam(s) {
		return nil, errors.New("access-key and secret-key must both be set to 'iam', or neither")
	} else if isEnv(a) || isEnv(s) {
		return nil, errors.New("access-key and secret-key must both be set to 'env', or neither")
	} else if isDefault(a) || isDefault(s) {
		return nil, errors.New("access-key and secret-key must both be set to 'default', or neither")
	} else {
		// Static credentials also benefit from caching wrapper
		return aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(a, s, "")), nil
	}
}

// isIam checks whether or not a variable is asking for iam
func isIam(key string) bool {
	return key == "iam"
}

// isEnv checks whether or not a variable is asking for env
func isEnv(key string) bool {
	return key == "env"
}

// isDefault checks whether or not a variable is asking for default
func isDefault(key string) bool {
	return key == "default"
}

// InterfaceToJSONString writes an interface as a JSON
func InterfaceToJSONString(m any, pretty bool) string {
	var b []byte
	var err error

	if pretty {
		b, err = json.MarshalIndent(m, "", "  ")
	} else {
		b, err = json.Marshal(m)
	}

	if err == nil {
		return string(b)
	}
	return "{}"
}

// StringInSlice checks whether or not a string is in an array
func StringInSlice(a string, list []string) bool {
	return slices.Contains(list, a)
}

// ReadGzFile reads a gzipped file
func ReadGzFile(filename string) (string, error) {
	fi, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer fi.Close()

	fz, err := gzip.NewReader(fi)
	if err != nil {
		return "", err
	}
	defer fz.Close()

	s, err := io.ReadAll(fz)
	if err != nil {
		return "", err
	}
	return string(s[:]), nil
}

// ReadGzFiles lists the files in dir and return their un-gzipped content
func ReadGzFiles(dir string) (map[string]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	for _, entry := range entries {
		content, err := ReadGzFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		m[entry.Name()] = content
	}
	return m, nil
}

// Diff outputs the difference between two string slices where a is the reference (a - b)
func Diff(a, b []string) []string {
	m := make(map[string]bool)
	for _, s := range a {
		m[s] = true
	}
	d := make([]string, 0)
	for _, s := range b {
		if !m[s] {
			d = append(d, s)
		}
	}
	return d
}
