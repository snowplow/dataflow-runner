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
	"compress/gzip"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
)

// GetCredentialsProvider attempts to fetch credentials from either:
// 1. IAM Role
// 2. ENV Variables
// 3. Static Credentials
func GetCredentialsProvider(a string, s string) (*credentials.Credentials, error) {
	if isIam(a) && isIam(s) {
		return credentials.NewCredentials(&ec2rolecreds.EC2RoleProvider{}), nil
	} else if isIam(a) || isIam(s) {
		return nil, errors.New("access-key and secret-key must both be set to 'iam', or neither")
	} else if isEnv(a) && isEnv(s) {
		return credentials.NewEnvCredentials(), nil
	} else if isEnv(a) || isEnv(s) {
		return nil, errors.New("access-key and secret-key must both be set to 'env', or neither")
	} else {
		return credentials.NewStaticCredentials(a, s, ""), nil
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

// InterfaceToJSONString writes an interface as a JSON
func InterfaceToJSONString(m interface{}, pretty bool) string {
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
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
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

	s, err := ioutil.ReadAll(fz)
	if err != nil {
		return "", err
	}
	return string(s[:]), nil
}

// ReadGzFiles lists the files in dir and return their un-gzipped content
func ReadGzFiles(dir string) (map[string]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	for _, file := range files {
		content, err := ReadGzFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		m[file.Name()] = content
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
