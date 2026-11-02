// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type ownershipCheckerFunc func(context.Context, string) error

func (f ownershipCheckerFunc) CheckUnused(ctx context.Context, name string) error {
	return f(ctx, name)
}

func TestOwnershipInspectionHonorsCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	checker := ownershipCheckerFunc(func(ctx context.Context, _ string) error { <-ctx.Done(); return ctx.Err() })
	err := checkTAPOwnership(ctx, checker, "tap0", time.Hour)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrTAPOwnershipTimeout) || !errors.Is(err, ErrTAPOwnershipInspection) {
		t.Fatalf("lost retryable timeout classification: %v", err)
	}
}
func TestOwnershipInspectionStopsBeforeScanOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := checkTAPOwnership(ctx, ownershipCheckerFunc(func(context.Context, string) error { called = true; return nil }), "tap0", time.Second)
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("called=%t err=%v", called, err)
	}
}
func TestOwnershipInspectionKeepsForeignOwnerDistinct(t *testing.T) {
	err := checkTAPOwnership(context.Background(), ownershipCheckerFunc(func(context.Context, string) error { return errTapOwned }), "tap0", time.Second)
	if !errors.Is(err, errTapOwned) || errors.Is(err, ErrTAPOwnershipInspection) {
		t.Fatalf("wrong owner classification: %v", err)
	}
}
func TestOwnershipCleanupRetryBackoff(t *testing.T) {
	pool, err := NewTapPool()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := NewTapPoolEntry("tap0", 1, net.IPv4(10, 0, 0, 2), "owner", TapPoolCleaning)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Add(entry); err != nil {
		t.Fatal(err)
	}
	for _, failure := range []error{errTapOwned, ErrTAPOwnershipInspection} {
		pool.RecordFailure("tap0", failure)
		current, _ := pool.GetByName("tap0")
		if pool.OwnershipRetryDue("tap0", current.OwnershipRetryAfter.Add(-time.Nanosecond)) || !pool.OwnershipRetryDue("tap0", current.OwnershipRetryAfter) {
			t.Fatal("retry gate ignored deadline")
		}
	}
	if ownershipRetryDelay(1) != 5*time.Second || ownershipRetryDelay(2) != 10*time.Second || ownershipRetryDelay(100) != 5*time.Minute {
		t.Fatal("retry delay is not bounded exponential backoff")
	}
	pool.RecordFailure("tap0", errors.New("unrelated egress failure"))
	if !pool.OwnershipRetryDue("tap0", time.Now()) {
		t.Fatal("ownership cooldown leaked into unrelated failures")
	}
}
