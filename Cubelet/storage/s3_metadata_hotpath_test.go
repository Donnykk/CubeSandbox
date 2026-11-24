// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tencentcloud/CubeSandbox/Cubelet/pkg/cubecow"
	"github.com/tencentcloud/CubeSandbox/Cubelet/storage/cow"
)

func TestS3MetadataParentRestoreDoesNotNeedNodeBase(t *testing.T) {
	stubS3MetadataMounts(t)
	engine := &fakeCowEngine{
		createVolumeErr: errors.New("node-local base is unavailable"),
		volumeInfos: map[string]*cubecow.Volume{
			S3MetadataSnapshotName("parent"): {SizeBytes: s3MetadataBaseSizeBytes},
		},
	}
	useTestCowStorage(t, engine)
	mount := t.TempDir()

	require.NoError(t, CloneS3MetadataFromParent(context.Background(), cow.BackendS3, "parent", "child", mount))
	require.Equal(t, [][2]string{{S3MetadataSnapshotName("parent"), S3MetadataVolumeName("child")}}, engine.createVolumeFromSnapshots)
	require.NotContains(t, engine.createVolumes, S3MetadataBaseVolumeName)
	require.Empty(t, engine.deactivatedVolumes)
	require.True(t, s3MetadataIsMounted(mount))
}
