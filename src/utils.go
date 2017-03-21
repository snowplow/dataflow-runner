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
	"encoding/json"
	"errors"
	"io/ioutil"

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

// FilePathToByteArray reads a file into a byte[]
func FilePathToByteArray(filePath string) ([]byte, error) {
	return ioutil.ReadFile(filePath)
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
