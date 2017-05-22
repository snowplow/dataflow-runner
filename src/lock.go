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
	"errors"
	"path/filepath"

	"github.com/hashicorp/consul/api"
	"github.com/nightlyone/lockfile"
)

// Lock interface abstracting over file-based or consul-based locks
type Lock interface {
	TryLock() error
	Unlock() error
}

// FileLock is for file-based locks
type FileLock struct {
	lock lockfile.Lockfile
}

// InitFileLock builds a FileLock at the path speicifed by name
func InitFileLock(name string) (Lock, error) {
	path, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	lock, err := lockfile.New(path)
	if err != nil {
		return nil, err
	}
	return &FileLock{lock: lock}, nil
}

// TryLock tries to acquire a lock on a file
func (fl FileLock) TryLock() error {
	return fl.lock.TryLock()
}

// Unlock tries to release the lock on a file
func (fl FileLock) Unlock() error {
	return fl.lock.Unlock()
}

// ConsulLock is for Consul-based locks
type ConsulLock struct {
	lock *api.Lock
}

// InitConsulLock builds a ConsulLock (a KV pair in Consul) with the name argument as key
func InitConsulLock(consulAddress, name string) (Lock, error) {
	client, err := api.NewClient(&api.Config{Address: consulAddress})
	if err != nil {
		return nil, err
	}

	opts := &api.LockOptions{
		Key:         name,
		LockTryOnce: true,
	}

	lock, err := client.LockOpts(opts)
	if err != nil {
		return nil, err
	}
	return &ConsulLock{lock: lock}, nil
}

// TryLock tries to acquire a lock from Consul
func (cl ConsulLock) TryLock() error {
	stopCh := make(chan struct{})
	leaderCh, err := cl.lock.Lock(stopCh)
	if err != nil {
		return err
	}
	if leaderCh == nil {
		return errors.New("Lock held by another process")
	}
	return nil
}

// Unlock tries to release the lock from Consul
func (cl ConsulLock) Unlock() error {
	return cl.lock.Unlock()
}
