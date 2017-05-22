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
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil"
	"github.com/stretchr/testify/assert"
)

func makeClient(t *testing.T) (*api.Client, *testutil.TestServer) {
	// Make client config
	conf := api.DefaultConfig()
	// Create server
	server, err := testutil.NewTestServerConfigT(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	conf.Address = server.HTTPAddr

	// Create client
	client, err := api.NewClient(conf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	return client, server
}

func TestConsulLock(t *testing.T) {
	assert := assert.New(t)

	c, s := makeClient(t)
	assert.NotNil(c)
	assert.NotNil(s)
	defer s.Stop()

	lockName := "lock"

	cl, err := InitConsulLock("some://faulty.address", lockName)
	assert.NotNil(err)
	assert.Nil(cl)
	assert.Equal("Unknown protocol scheme: some", err.Error())

	cl, err = InitConsulLock(s.HTTPAddr, lockName)
	assert.Nil(err)
	assert.NotNil(cl)

	err = cl.TryLock()
	assert.Nil(err)

	// fail if already locked
	err = cl.TryLock()
	assert.NotNil(err)
	assert.Equal("Lock currently held at "+lockName, err.Error())

	// fail if already locked by another lock
	ocl, err := InitConsulLock(s.HTTPAddr, lockName)
	assert.Nil(err)
	assert.NotNil(ocl)
	err = ocl.TryLock()
	assert.NotNil(err)
	assert.Equal("Lock currently held at "+lockName, err.Error())

	// fail if already unlocked
	err = cl.Unlock()
	assert.Nil(err)

	err = cl.Unlock()
	assert.NotNil(err)
	assert.Equal(api.ErrLockNotHeld, err)
}

func TestFileLock(t *testing.T) {
	assert := assert.New(t)

	lockPath := "/tmp/lock"

	fl, err := InitFileLock(lockPath)
	assert.NotNil(fl)
	assert.Nil(err)
	assert.Equal(fl, &FileLock{path: lockPath})

	// write to the file so that we can't get a lock on it
	pid := os.Getppid()
	err = ioutil.WriteFile(lockPath, []byte(strconv.Itoa(pid)+"\n"), 0666)
	assert.Nil(err)

	err = fl.TryLock()
	assert.NotNil(err)
	assert.Equal("Lock currently held at "+lockPath, err.Error())

	// cleanup
	err = os.Remove(lockPath)
	assert.Nil(err)

	err = fl.TryLock()
	assert.Nil(err)

	err = fl.Unlock()
	assert.Nil(err)

	err = fl.Unlock()
	assert.NotNil(err)
	assert.Equal("remove "+lockPath+": no such file or directory", err.Error())
}

func TestGetLock(t *testing.T) {
	assert := assert.New(t)

	c, s := makeClient(t)
	assert.NotNil(c)
	assert.NotNil(s)
	defer s.Stop()

	lockName := "/tmp/lock"

	// FileLock if consul == ""
	lock, err := GetLock(lockName, "")
	assert.NotNil(lock)
	assert.Nil(err)
	assert.Equal(lock, &FileLock{path: lockName})

	// ConsulLock if consul != ""
	lock, err = GetLock(lockName, s.HTTPAddr)
	assert.NotNil(lock)
	assert.Nil(err)
	cl, ok := lock.(*ConsulLock)
	assert.NotNil(cl)
	assert.Equal(true, ok)

	// error otherwise
	lock, err = GetLock(lockName, "some://faulty.address")
	assert.Nil(lock)
	assert.NotNil(err)
	assert.Equal("Unknown protocol scheme: some", err.Error())
}
