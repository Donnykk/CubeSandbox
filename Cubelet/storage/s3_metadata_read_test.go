// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
	"golang.org/x/sys/unix"
)

func metadataReadFixture(t *testing.T) (string, string, *fakeCowEngine) {
	t.Helper()
	stubS3MetadataMounts(t)
	id := "read-package"
	engine := &fakeCowEngine{volumeInfos: map[string]*cubecow.Volume{
		S3MetadataVolumeName(id): {SizeBytes: s3MetadataBaseSizeBytes, DevicePath: "/dev/test-metadata"},
	}}
	previous := localStorage
	localStorage = &local{config: &Config{StorageBackend: "cubecow"}, s3CowManager: &S3Cow{engine: engine}}
	t.Cleanup(func() { localStorage = previous })
	return id, filepath.Join(t.TempDir(), id, SnapshotMetadataDir), engine
}

func TestReadS3PackageMetadataReleasesOwnedMount(t *testing.T) {
	id, path, engine := metadataReadFixture(t)
	err := ReadS3PackageMetadata(t.Context(), cow.BackendS3, id, path, func() error {
		require.True(t, s3MetadataIsMounted(path))
		// Inventory readers must be able to take the state lock in the callback.
		require.False(t, S3PackageOperationInProgress(id))
		return nil
	})
	require.NoError(t, err)
	require.False(t, s3MetadataIsMounted(path))
	require.NotContains(t, s3MetadataMounts, id)
	require.Contains(t, engine.deactivatedVolumes, S3MetadataVolumeName(id))
	require.Empty(t, engine.deletedVolumes)
}

func TestReadS3PackageMetadataPreservesExistingMount(t *testing.T) {
	id, path, engine := metadataReadFixture(t)
	require.NoError(t, MountS3MetadataAt(t.Context(), cow.BackendS3, id, path))
	before := len(engine.deactivatedVolumes)
	require.NoError(t, ReadS3PackageMetadata(t.Context(), cow.BackendS3, id, path, func() error { return nil }))
	require.True(t, s3MetadataIsMounted(path))
	require.Len(t, engine.deactivatedVolumes, before)
}

func TestReadS3PackageMetadataRejectsAnUnrelatedMount(t *testing.T) {
	id, path, engine := metadataReadFixture(t)
	require.NoError(t, mountS3MetadataDevice("/dev/unrelated", path))
	err := ReadS3PackageMetadata(t.Context(), cow.BackendS3, id, path, func() error {
		t.Fatal("must not read another package's metadata")
		return nil
	})
	require.ErrorContains(t, err, "does not match package")
	require.True(t, s3MetadataIsMounted(path))
	require.Empty(t, engine.deactivatedVolumes)
}

func TestReadS3PackageMetadataCleansUpAfterReadError(t *testing.T) {
	id, path, engine := metadataReadFixture(t)
	want := errors.New("metadata read failed")
	err := ReadS3PackageMetadata(t.Context(), cow.BackendS3, id, path, func() error { return want })
	require.ErrorIs(t, err, want)
	require.False(t, s3MetadataIsMounted(path))
	require.Contains(t, engine.deactivatedVolumes, S3MetadataVolumeName(id))
}

func TestReadS3PackageMetadataKeepsBusyAttachment(t *testing.T) {
	id, path, engine := metadataReadFixture(t)
	unmountS3PackageRead = func(string) error { return unix.EBUSY }
	require.NoError(t, ReadS3PackageMetadata(t.Context(), cow.BackendS3, id, path, func() error { return nil }))
	require.True(t, s3MetadataIsMounted(path))
	require.Equal(t, path, s3MetadataMounts[id])
	require.Empty(t, engine.deactivatedVolumes)
}

func TestReadS3PackageMetadataRejectsActiveWriterAndCanceledContext(t *testing.T) {
	id, path, engine := metadataReadFixture(t)
	_, release, err := BeginS3PackageWrite(t.Context(), cow.BackendS3, id)
	require.NoError(t, err)
	t.Cleanup(release)
	err = ReadS3PackageMetadata(t.Context(), cow.BackendS3, id, path, func() error {
		t.Fatal("read must not run during a package write")
		return nil
	})
	require.ErrorContains(t, err, "operation in progress")
	release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, ReadS3PackageMetadata(ctx, cow.BackendS3, id, path, func() error { return nil }), context.Canceled)
	require.Empty(t, engine.activatedVolumes)
}

func TestReadS3PackageMetadataRejectsAnUninitializedStore(t *testing.T) {
	previous := localStorage
	localStorage = nil
	t.Cleanup(func() { localStorage = previous })
	err := ReadS3PackageMetadata(t.Context(), cow.BackendS3, "package", t.TempDir(), func() error {
		t.Fatal("read must not run before the store is initialized")
		return nil
	})
	require.Error(t, err)
}

func TestReadS3PackageMetadataExcludesCleanupUntilReadFinishes(t *testing.T) {
	id, path, _ := metadataReadFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	readDone, cleanupDone := make(chan error, 1), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	var workers sync.WaitGroup
	t.Cleanup(func() { unblock(); workers.Wait() })
	workers.Add(1)
	go func() {
		defer workers.Done()
		readDone <- ReadS3PackageMetadata(context.Background(), cow.BackendS3, id, path, func() error {
			close(entered)
			<-resume
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("metadata read did not start")
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		release, err := BeginS3PackageCleanup(context.Background(), cow.BackendS3, id)
		if err == nil {
			release()
		}
		cleanupDone <- err
	}()
	select {
	case err := <-cleanupDone:
		t.Fatalf("cleanup overtook the active read: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-readDone)
	require.NoError(t, <-cleanupDone)
	require.False(t, s3MetadataIsMounted(path))
}
