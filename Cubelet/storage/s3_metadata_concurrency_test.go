// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

type blockingMetadataEngine struct {
	*fakeCowEngine
	mu          sync.Mutex
	blockLookup string
	blockClone  string
	entered     chan struct{}
	resume      chan struct{}
	paused      atomic.Bool
}

func (e *blockingMetadataEngine) pause() {
	if e.paused.CompareAndSwap(false, true) {
		close(e.entered)
		<-e.resume
	}
}

func (e *blockingMetadataEngine) GetVolumeInfo(name string) (*cubecow.Volume, error) {
	if name == e.blockLookup {
		e.pause()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	info, err := e.fakeCowEngine.GetVolumeInfo(name)
	if info == nil {
		return nil, err
	}
	copy := *info
	return &copy, err
}

func (e *blockingMetadataEngine) CreateVolumeFromSnapshot(source, name string) (string, error) {
	if name == e.blockClone {
		e.pause()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fakeCowEngine.CreateVolumeFromSnapshot(source, name)
}

func (e *blockingMetadataEngine) DeactivateVolume(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fakeCowEngine.DeactivateVolume(name)
}

func (e *blockingMetadataEngine) DeleteVolume(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fakeCowEngine.DeleteVolume(name)
}

func newBlockingMetadataEngine(t *testing.T) *blockingMetadataEngine {
	t.Helper()
	stubS3MetadataMounts(t)
	e := &blockingMetadataEngine{
		fakeCowEngine: &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{
			S3MetadataBaseVolumeName:           {SizeBytes: s3MetadataBaseSizeBytes},
			S3MetadataBaseSnapshotName:         {SizeBytes: s3MetadataBaseSizeBytes},
			S3MetadataSnapshotName("parent-a"): {SizeBytes: s3MetadataBaseSizeBytes},
			S3MetadataSnapshotName("parent-b"): {SizeBytes: s3MetadataBaseSizeBytes},
		}},
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	previous := localStorage
	localStorage = &local{config: &Config{StorageBackend: "cubecow"}, s3CowManager: &S3Cow{engine: e}}
	t.Cleanup(func() { localStorage = previous })
	return e
}

func TestS3MetadataClonesDoNotSerializeStorageIO(t *testing.T) {
	for _, stage := range []string{"lookup", "clone", "mount"} {
		t.Run(stage, func(t *testing.T) {
			e := newBlockingMetadataEngine(t)
			firstMount, secondMount := t.TempDir(), t.TempDir()
			if stage == "lookup" {
				e.blockLookup = S3MetadataSnapshotName("parent-a")
			} else if stage == "clone" {
				e.blockClone = S3MetadataVolumeName("child-a")
			} else {
				mount := mountS3MetadataDevice
				mountS3MetadataDevice = func(device, target string) error {
					if target == firstMount {
						e.pause()
					}
					return mount(device, target)
				}
			}
			first, second := make(chan error, 1), make(chan error, 1)
			var release sync.Once
			resume := func() { release.Do(func() { close(e.resume) }) }
			var wg sync.WaitGroup
			t.Cleanup(func() { resume(); wg.Wait() })
			wg.Add(1)
			go func() {
				defer wg.Done()
				first <- CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-a", firstMount)
			}()
			select {
			case <-e.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("first clone did not enter the storage operation")
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				second <- CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-b", secondMount)
			}()
			select {
			case err := <-second:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("unrelated child was blocked by another clone's storage operation")
			}
			resume()
			require.NoError(t, <-first)
			s3MetadataMu.Lock()
			defer s3MetadataMu.Unlock()
			state, err := loadS3MetadataState()
			require.NoError(t, err)
			require.Contains(t, state.Derived, "child-a")
			require.Contains(t, state.Derived, "child-b")
		})
	}
}

func TestS3MetadataCloneRetainsMutationExclusion(t *testing.T) {
	e := newBlockingMetadataEngine(t)
	e.blockClone = S3MetadataVolumeName("child-a")
	mount := t.TempDir()
	first, mutation := make(chan error, 1), make(chan error, 1)
	var release sync.Once
	resume := func() { release.Do(func() { close(e.resume) }) }
	var wg sync.WaitGroup
	t.Cleanup(func() { resume(); wg.Wait() })
	wg.Add(1)
	go func() {
		defer wg.Done()
		first <- CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-a", mount)
	}()
	select {
	case <-e.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first clone did not enter the storage RPC")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, CloneS3MetadataFromParent(ctx, cow.BackendS3, "parent-a", "child-a", mount), context.Canceled)
	wg.Add(1)
	go func() {
		defer wg.Done()
		mutation <- ReleaseS3MetadataDerivedVolume(context.Background(), cow.BackendS3, "child-a")
	}()
	select {
	case err := <-mutation:
		t.Fatalf("release overtook an in-flight clone: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	resume()
	require.NoError(t, <-first)
	require.NoError(t, <-mutation)
	require.Len(t, e.createVolumeFromSnapshots, 1)
	require.Contains(t, e.deletedVolumes, S3MetadataVolumeName("child-a"))
}

func TestS3MetadataCloneSerializesMountAliases(t *testing.T) {
	e := newBlockingMetadataEngine(t)
	target := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(target, alias))
	mount := mountS3MetadataDevice
	var mountCalls atomic.Int32
	mountS3MetadataDevice = func(device, path string) error {
		mountCalls.Add(1)
		e.pause()
		return mount(device, path)
	}
	first := make(chan error, 1)
	var release sync.Once
	resume := func() { release.Do(func() { close(e.resume) }) }
	var wg sync.WaitGroup
	t.Cleanup(func() { resume(); wg.Wait() })
	wg.Add(1)
	go func() {
		defer wg.Done()
		first <- CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-a", target)
	}()
	select {
	case <-e.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first clone did not start mounting")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, CloneS3MetadataFromParent(ctx, cow.BackendS3, "parent-a", "child-b", alias), context.DeadlineExceeded)
	require.Equal(t, int32(1), mountCalls.Load())
	resume()
	require.NoError(t, <-first)
}

func TestS3MetadataParentCloneDoesNotReconcileNodeBase(t *testing.T) {
	e := newBlockingMetadataEngine(t)
	e.blockLookup = S3MetadataBaseVolumeName
	done := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	mount := t.TempDir()
	t.Cleanup(func() { close(e.resume); wg.Wait() })
	go func() {
		defer wg.Done()
		done <- CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-a", mount)
	}()
	select {
	case <-e.entered:
		t.Fatal("restoring an existing parent reconciled the unused node-local base")
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("restoring an existing parent stalled")
	}
	require.Empty(t, e.deactivatedVolumes)
}

func TestS3MetadataMountReconciliationDoesNotBlockOtherChildren(t *testing.T) {
	for _, stage := range []string{"unmount", "mount"} {
		t.Run(stage, func(t *testing.T) {
			e := newBlockingMetadataEngine(t)
			previous, target, other := t.TempDir(), t.TempDir(), t.TempDir()
			require.NoError(t, CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-a", previous))
			mount, unmount := mountS3MetadataDevice, unmountS3MetadataPath
			mountS3MetadataDevice = func(device, path string) error {
				if stage == "mount" && path == target {
					e.pause()
				}
				return mount(device, path)
			}
			unmountS3MetadataPath = func(path string) error {
				if stage == "unmount" && path == previous {
					e.pause()
				}
				return unmount(path)
			}
			var resume sync.Once
			unblock := func() { resume.Do(func() { close(e.resume) }) }
			var wg sync.WaitGroup
			t.Cleanup(func() { unblock(); wg.Wait() })
			first, second := make(chan error, 1), make(chan error, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				first <- CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-a", target)
			}()
			select {
			case <-e.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("mount reconciliation did not reach the blocked operation")
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				second <- CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent-a", "child-b", other)
			}()
			select {
			case err := <-second:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("mount reconciliation blocked another child")
			}
			unblock()
			require.NoError(t, <-first)
			require.False(t, s3MetadataIsMounted(previous))
			require.Equal(t, target, s3MetadataMounts["child-a"])
		})
	}
}
