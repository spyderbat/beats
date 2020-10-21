// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package memlog

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/statestore/backend"
)

// Registry configures access to memlog based stores.
type Registry struct {
	log *logp.Logger

	mu     sync.Mutex
	active bool

	settings Settings

	wg sync.WaitGroup
}

// Settings configures a new Registry.
type Settings struct {
	// Registry root directory. Stores will be single sub-directories.
	Root string

	// FileMode is used to configure the file mode for new files generated by the
	// regisry.  File mode 0600 will be used if this field is not set.
	FileMode os.FileMode

	// BufferSize configures the IO buffer size when accessing the underlying
	// storage files.  Defaults to 4096 if not set.
	BufferSize uint

	// Checkpoint predicate that can trigger a registry file rotation.  If not
	// configured, memlog will automatically trigger a checkpoint every 10MB.
	Checkpoint CheckpointPredicate

	// If set memlog will not check the version of the meta file.
	IgnoreVersionCheck bool
}

// CheckpointPredicate is the type for configurable checkpoint checks.
// The store executes a checkpoint operation when the predicate returns true.
type CheckpointPredicate func(fileSize uint64) bool

const defaultFileMode os.FileMode = 0600

const defaultBufferSize = 4 * 1024

func defaultCheckpoint(filesize uint64) bool {
	const limit = 10 * 1 << 20 // set rotation limit to 10MB by default
	return filesize >= limit
}

// New configures a memlog Registry that can be used to open stores.
func New(log *logp.Logger, settings Settings) (*Registry, error) {
	if settings.FileMode == 0 {
		settings.FileMode = defaultFileMode
	}
	if settings.Checkpoint == nil {
		settings.Checkpoint = defaultCheckpoint
	}
	if settings.BufferSize == 0 {
		settings.BufferSize = defaultBufferSize
	}

	root, err := filepath.Abs(settings.Root)
	if err != nil {
		return nil, err
	}

	settings.Root = root
	return &Registry{
		log:      log,
		active:   true,
		settings: settings,
	}, nil
}

// Access creates or opens a new store. A new sub-directory for the store if
// created, if the store does not exist.
// Returns an error is any file access fails.
func (r *Registry) Access(name string) (backend.Store, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.active {
		return nil, errRegClosed
	}

	logger := r.log.With("store", name)

	home := filepath.Join(r.settings.Root, name)
	fileMode := r.settings.FileMode
	bufSz := r.settings.BufferSize
	store, err := openStore(logger, home, fileMode, bufSz, r.settings.IgnoreVersionCheck, r.settings.Checkpoint)
	if err != nil {
		return nil, err
	}

	return store, nil
}

// Close closes the registry. No new store can be accessed during close.
// Close blocks until all stores have been closed.
func (r *Registry) Close() error {
	r.mu.Lock()
	r.active = false
	r.mu.Unlock()

	// block until all stores have been closed
	r.wg.Wait()
	return nil
}
