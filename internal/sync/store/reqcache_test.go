package store

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestMemoCallsLoaderOnce(t *testing.T) {
	ctx := WithRequestCache(context.Background())
	calls := 0
	load := func() (string, error) { calls++; return "v", nil }

	for i := 0; i < 3; i++ {
		got, err := memo(ctx, "k", load)
		if err != nil || got != "v" {
			t.Fatalf("memo = %q, %v", got, err)
		}
	}
	if calls != 1 {
		t.Fatalf("loader ran %d times, want 1", calls)
	}
}

// Without a cache in the context the loader must still run, every time. Store
// methods are called from tests and from paths that never install one.
func TestMemoWithoutCacheFallsThrough(t *testing.T) {
	calls := 0
	for i := 0; i < 3; i++ {
		if _, err := memo(context.Background(), "k", func() (int, error) { calls++; return 1, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 {
		t.Fatalf("loader ran %d times, want 3", calls)
	}
}

// Two passes of a long poll must not share answers: each installs its own.
func TestMemoIsNotSharedBetweenPasses(t *testing.T) {
	req := context.Background()
	calls := 0
	load := func() (int, error) { calls++; return calls, nil }

	for pass := 0; pass < 2; pass++ {
		ctx := WithRequestCache(req)
		if _, err := memo(ctx, "k", load); err != nil {
			t.Fatal(err)
		}
		if _, err := memo(ctx, "k", load); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("loader ran %d times, want one per pass", calls)
	}
}

// A failed query must not be remembered: one transient error would otherwise
// become an error for everything else that asks the same question.
func TestMemoDoesNotCacheErrors(t *testing.T) {
	ctx := WithRequestCache(context.Background())
	fail := errors.New("boom")
	calls := 0
	load := func() (int, error) {
		calls++
		if calls == 1 {
			return 0, fail
		}
		return 7, nil
	}

	if _, err := memo(ctx, "k", load); !errors.Is(err, fail) {
		t.Fatalf("first call err = %v, want boom", err)
	}
	got, err := memo(ctx, "k", load)
	if err != nil || got != 7 {
		t.Fatalf("second call = %d, %v; want 7, nil", got, err)
	}
}

// Rooms are built ten at a time, so the memo is written concurrently.
func TestMemoIsSafeUnderConcurrency(t *testing.T) {
	ctx := WithRequestCache(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := memo(ctx, "k", func() (int, error) { return 1, nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestMemoStopsStoringAtTheCap(t *testing.T) {
	ctx := WithRequestCache(context.Background())
	for i := 0; i < maxRequestCacheEntries+10; i++ {
		if _, err := memo(ctx, string(rune(i))+"x", func() (int, error) { return i, nil }); err != nil {
			t.Fatal(err)
		}
	}
	c := ctx.Value(reqCacheKey{}).(*requestCache)
	if len(c.m) > maxRequestCacheEntries {
		t.Fatalf("cache holds %d entries, cap is %d", len(c.m), maxRequestCacheEntries)
	}
}
