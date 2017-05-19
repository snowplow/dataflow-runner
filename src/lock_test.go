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

	"github.com/nightlyone/lockfile"
	"github.com/stretchr/testify/assert"
)

func TestFileLock(t *testing.T) {
	assert := assert.New(t)

	lockPath := "/tmp/lock"

	fl, err := InitFileLock(lockPath)
	assert.NotNil(fl)
	assert.Nil(err)
	assert.Equal(fl, &FileLock{lock: lockfile.Lockfile(lockPath)})

	// write to the file so that we can't get a lock on it
	pid := os.Getppid()
	err = ioutil.WriteFile(lockPath, []byte(strconv.Itoa(pid)+"\n"), 0666)
	assert.Nil(err)

	err = fl.TryLock()
	assert.NotNil(err)
	assert.Equal(lockfile.ErrBusy, err)

	// cleanup
	err = os.Remove(lockPath)
	assert.Nil(err)

	err = fl.TryLock()
	assert.Nil(err)

	err = fl.Unlock()
	assert.Nil(err)

	err = fl.Unlock()
	assert.NotNil(err)
	assert.Equal(lockfile.ErrRogueDeletion, err)
}
