// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resolver_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/mcp-toolbox/internal/server/primitives"
	"github.com/googleapis/mcp-toolbox/internal/server/resolver"
	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// countingSourceConfig records how many times Initialize was called and can be
// told to fail, so tests can observe caching and retry behavior.
type countingSourceConfig struct {
	mu     sync.Mutex
	calls  int
	err    error
	delay  time.Duration
	ctxErr error
}

func (c *countingSourceConfig) SourceConfigType() string { return "counting" }

func (c *countingSourceConfig) Initialize(ctx context.Context, _ trace.Tracer) (sources.Source, error) {
	c.mu.Lock()
	c.calls++
	err, delay := c.err, c.delay
	c.mu.Unlock()

	// Holding the connection open lets concurrent callers pile up behind it,
	// so a missing singleflight shows up as extra Initialize calls. Watching
	// ctx here is what lets a test observe whether the shared attempt inherited
	// a caller's cancellation.
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		c.mu.Lock()
		c.ctxErr = ctx.Err()
		c.mu.Unlock()
		return nil, ctx.Err()
	}

	if err != nil {
		return nil, err
	}
	return testutils.MockSource{MockSourceConfig: testutils.MockSourceConfig{Name: "counted", Type: "counting"}}, nil
}

func (c *countingSourceConfig) initContextErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctxErr
}

func (c *countingSourceConfig) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// newLazyResolver returns a resolver over an empty store, plus the store so a
// test can assert what became visible to listing paths.
func newLazyResolver(configs map[string]sources.SourceConfig) (*resolver.SourceResolver, *primitives.PrimitiveManager) {
	mgr := primitives.NewPrimitiveManager(nil, nil, nil, nil, nil, nil)
	r := resolver.New(mgr)
	r.SetLazySources(configs, noop.NewTracerProvider().Tracer("test"))
	return r, mgr
}

func TestResolveConnectsOnceUnderConcurrency(t *testing.T) {
	cfg := &countingSourceConfig{delay: 50 * time.Millisecond}
	r, mgr := newLazyResolver(map[string]sources.SourceConfig{"lazy": cfg})

	// Before the first resolve the source must be invisible to listing paths,
	// which is what lets tools/list work without connectivity.
	if _, ok := mgr.GetSource("lazy"); ok {
		t.Fatal("expected an unconnected source to be absent from GetSource")
	}

	const callers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = r.Resolve(context.Background(), "lazy")
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d failed to resolve: %s", i, err)
		}
	}
	if got := cfg.callCount(); got != 1 {
		t.Fatalf("expected the racing callers to share one Initialize, got %d", got)
	}
	if _, ok := mgr.GetSource("lazy"); !ok {
		t.Fatal("expected the connected source to be visible to GetSource")
	}
}

func TestResolveSurvivesFirstCallerCancellation(t *testing.T) {
	cfg := &countingSourceConfig{delay: 100 * time.Millisecond}
	r, mgr := newLazyResolver(map[string]sources.SourceConfig{"lazy": cfg})

	// The first caller starts the shared attempt and then walks away. Its
	// context must not travel into Initialize, or every other caller waiting on
	// the same attempt fails for a request that is no longer theirs.
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstStarted := make(chan struct{})
	firstErr := make(chan error, 1)
	go func() {
		close(firstStarted)
		_, err := r.Resolve(firstCtx, "lazy")
		firstErr <- err
	}()
	<-firstStarted

	secondErr := make(chan error, 1)
	go func() {
		// Give the first caller time to win the singleflight.
		time.Sleep(20 * time.Millisecond)
		cancelFirst()
		_, err := r.Resolve(context.Background(), "lazy")
		secondErr <- err
	}()

	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the cancelled caller to see its own cancellation, got %v", err)
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("a cancelled caller must not fail the others waiting: %s", err)
	}
	if err := cfg.initContextErr(); err != nil {
		t.Fatalf("Initialize saw a cancelled context: %s", err)
	}
	if _, ok := mgr.GetSource("lazy"); !ok {
		t.Fatal("expected the shared attempt to finish and cache the source")
	}
}

func TestResolveRespectsCallerDeadline(t *testing.T) {
	cfg := &countingSourceConfig{delay: time.Minute}
	r, _ := newLazyResolver(map[string]sources.SourceConfig{"lazy": cfg})

	// A hung connect must not pin the request for SourceInitTimeout: the caller
	// returns on its own deadline and the agent gets a readable error.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := r.Resolve(ctx, "lazy")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the caller's deadline to end the wait, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("caller waited %s, well past its own deadline", elapsed)
	}
}

func TestResolveRetriesAfterFailure(t *testing.T) {
	cfg := &countingSourceConfig{err: errors.New("connection refused")}
	r, mgr := newLazyResolver(map[string]sources.SourceConfig{"lazy": cfg})

	if _, err := r.Resolve(context.Background(), "lazy"); err == nil {
		t.Fatal("expected the first resolve to fail")
	}
	if _, ok := mgr.GetSource("lazy"); ok {
		t.Fatal("a failed source must not be cached")
	}

	// A failure is not cached, so a source that comes up later starts working
	// without restarting the server.
	cfg.mu.Lock()
	cfg.err = nil
	cfg.mu.Unlock()

	if _, err := r.Resolve(context.Background(), "lazy"); err != nil {
		t.Fatalf("expected the retry to succeed, got %s", err)
	}
	if got := cfg.callCount(); got != 2 {
		t.Fatalf("expected 2 Initialize calls, got %d", got)
	}
}

func TestResolveUnknownName(t *testing.T) {
	cfg := &countingSourceConfig{}
	r, _ := newLazyResolver(map[string]sources.SourceConfig{"lazy": cfg})

	if _, err := r.Resolve(context.Background(), "nonexistent"); err == nil {
		t.Fatal("expected an error for an unconfigured source")
	}
	if got := cfg.callCount(); got != 0 {
		t.Fatalf("expected no Initialize calls, got %d", got)
	}
}

func TestResolveEager(t *testing.T) {
	// Without SetLazySources the resolver only serves already-connected
	// sources, which is the behavior every invocation path gets by default.
	src := testutils.MockSource{MockSourceConfig: testutils.MockSourceConfig{Name: "eager", Type: "mock"}}
	mgr := primitives.NewPrimitiveManager(map[string]sources.Source{"eager": src}, nil, nil, nil, nil, nil)
	r := resolver.New(mgr)

	if r.Lazy() {
		t.Fatal("expected lazy sources to be off by default")
	}
	got, err := r.Resolve(context.Background(), "eager")
	if err != nil {
		t.Fatalf("unexpected error resolving a connected source: %s", err)
	}
	if diff := cmp.Diff(got, sources.Source(src)); diff != "" {
		t.Errorf("unexpected source (-want +got):\n%s", diff)
	}
	if _, err := r.Resolve(context.Background(), "missing"); err == nil {
		t.Fatal("expected an error for a source that was never initialized")
	}
}
