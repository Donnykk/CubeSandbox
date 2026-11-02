// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package diag

import "testing"

func TestReclaimProtectsCubeletAndSystemProcesses(t *testing.T) {
	for _, tc := range []struct {
		pid int
		exe string
	}{{1, "/sbin/init"}, {99, "/bin/cubecli"}, {42, "/usr/bin/cubelet"}, {42, "/usr/bin/cubelet-release (deleted)"}} {
		if err := validateReclaimTarget(tc.pid, 99, tc.exe); err == nil {
			t.Fatalf("accepted protected target: %+v", tc)
		}
	}
	if err := validateReclaimTarget(42, 99, "/usr/bin/containerd-shim-cube-rs"); err != nil {
		t.Fatal(err)
	}
}
