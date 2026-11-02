// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	errTapOwned = errors.New("TAP has a foreign descriptor owner")
	// ErrTAPOwnershipInspection means reuse could not be certified; retry later.
	ErrTAPOwnershipInspection = errors.New("TAP ownership inspection unavailable")
	// ErrTAPOwnershipTimeout distinguishes a retryable scan deadline from a holder.
	ErrTAPOwnershipTimeout = errors.New("TAP ownership inspection timed out")
)

type tapOwnershipChecker interface {
	CheckUnused(context.Context, string) error
}

func ownershipTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return 2 * time.Second
}

func checkTAPOwnership(ctx context.Context, checker tapOwnershipChecker, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, ownershipTimeout(timeout))
	defer cancel()
	err := ctx.Err()
	if err == nil {
		err = checker.CheckUnused(ctx, name)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil || errors.Is(err, errTapOwned) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		err = errors.Join(ErrTAPOwnershipTimeout, err)
	}
	return fmt.Errorf("%w: %w", ErrTAPOwnershipInspection, err)
}

func ownershipRetryDelay(failures int) time.Duration {
	delay := 5 * time.Second
	for i := 1; i < failures && delay < 5*time.Minute; i++ {
		delay *= 2
	}
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}
