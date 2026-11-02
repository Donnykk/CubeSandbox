// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package diag

import (
	"fmt"
	"path/filepath"
	"strings"
)

func validateReclaimTarget(pid, self int, executable string) error {
	name := strings.ToLower(strings.TrimSuffix(filepath.Base(executable), " (deleted)"))
	if pid <= 1 || pid == self || name == "cubelet" || strings.HasPrefix(name, "cubelet-") {
		return fmt.Errorf("refusing to reclaim protected process %d (%s)", pid, name)
	}
	return nil
}
