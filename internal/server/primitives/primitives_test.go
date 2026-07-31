// Copyright 2025 Google LLC
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

package primitives_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/mcp-toolbox/internal/auth"
	"github.com/googleapis/mcp-toolbox/internal/embeddingmodels"
	"github.com/googleapis/mcp-toolbox/internal/group"
	"github.com/googleapis/mcp-toolbox/internal/prompts"
	"github.com/googleapis/mcp-toolbox/internal/server/primitives"
	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/sources/alloydbpg"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/internal/tools"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestUpdateServer(t *testing.T) {
	newSources := map[string]sources.Source{
		"example-source": &alloydbpg.Source{
			Config: alloydbpg.Config{
				Name: "example-alloydb-source",
				Type: "alloydb-postgres",
			},
		},
	}
	newAuth := map[string]auth.AuthService{"example-auth": nil}
	newEmbeddingModels := map[string]embeddingmodels.EmbeddingModel{"example-model": nil}
	newTools := map[string]tools.Tool{"example-tool": nil}
	newPrompts := map[string]prompts.Prompt{"example-prompt": testutils.NewMockPrompt("example-prompt", "", prompts.Arguments{})}
	newGroups := map[string]group.Group{
		"example-toolset": group.NewGroup(group.GroupConfig{Name: "example-toolset", ToolNames: []string{"example-tool"}}),
	}
	resMgr := primitives.NewPrimitiveManager(newSources, newAuth, newEmbeddingModels, newTools, newPrompts, newGroups)

	gotSource, _ := resMgr.GetSource("example-source")
	if diff := cmp.Diff(gotSource, newSources["example-source"]); diff != "" {
		t.Errorf("error updating server, sources (-want +got):\n%s", diff)
	}

	gotAuthService, _ := resMgr.GetAuthService("example-auth")
	if diff := cmp.Diff(gotAuthService, newAuth["example-auth"]); diff != "" {
		t.Errorf("error updating server, authServices (-want +got):\n%s", diff)
	}

	gotTool, _ := resMgr.GetTool("example-tool")
	if diff := cmp.Diff(gotTool, newTools["example-tool"]); diff != "" {
		t.Errorf("error updating server, tools (-want +got):\n%s", diff)
	}

	wantGroup := newGroups["example-toolset"]
	gotGroup, ok := resMgr.GetGroup("example-toolset")
	if !ok {
		t.Fatal("expected group \"example-toolset\" to exist")
	}
	if diff := cmp.Diff(wantGroup, gotGroup, cmp.AllowUnexported(group.Group{})); diff != "" {
		t.Errorf("error updating server, group (-want +got):\n%s", diff)
	}

	gotPrompt, _ := resMgr.GetPrompt("example-prompt")
	if diff := cmp.Diff(gotPrompt, newPrompts["example-prompt"], cmp.AllowUnexported(testutils.MockPrompt{})); diff != "" {
		t.Errorf("error updating server, prompts (-want +got):\n%s", diff)
	}

	updateSource := map[string]sources.Source{
		"example-source2": &alloydbpg.Source{
			Config: alloydbpg.Config{
				Name: "example-alloydb-source2",
				Type: "alloydb-postgres",
			},
		},
	}

	resMgr.SetPrimitives(updateSource, newAuth, newEmbeddingModels, newTools, newPrompts, newGroups)
	gotSource, _ = resMgr.GetSource("example-source2")
	if diff := cmp.Diff(gotSource, updateSource["example-source2"]); diff != "" {
		t.Errorf("error updating server, sources (-want +got):\n%s", diff)
	}
}

// countingSourceConfig records how many times Initialize was called and can be
// told to fail, so tests can observe caching and retry behavior.
type countingSourceConfig struct {
	mu    sync.Mutex
	calls int
	err   error
	delay time.Duration
}

func (c *countingSourceConfig) SourceConfigType() string { return "counting" }

func (c *countingSourceConfig) Initialize(context.Context, trace.Tracer) (sources.Source, error) {
	c.mu.Lock()
	c.calls++
	err, delay := c.err, c.delay
	c.mu.Unlock()

	// Holding the connection open lets concurrent callers pile up behind it,
	// so a missing singleflight shows up as extra Initialize calls.
	time.Sleep(delay)

	if err != nil {
		return nil, err
	}
	return testutils.MockSource{MockSourceConfig: testutils.MockSourceConfig{Name: "counted", Type: "counting"}}, nil
}

func (c *countingSourceConfig) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newLazyManager(configs map[string]sources.SourceConfig) *primitives.PrimitiveManager {
	mgr := primitives.NewPrimitiveManager(nil, nil, nil, nil, nil, nil)
	mgr.SetLazySources(configs, noop.NewTracerProvider().Tracer("test"))
	return mgr
}

func TestResolveSourceConnectsOnceUnderConcurrency(t *testing.T) {
	cfg := &countingSourceConfig{delay: 50 * time.Millisecond}
	mgr := newLazyManager(map[string]sources.SourceConfig{"lazy": cfg})

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
			_, errs[i] = mgr.ResolveSource(context.Background(), "lazy")
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

func TestResolveSourceRetriesAfterFailure(t *testing.T) {
	cfg := &countingSourceConfig{err: errors.New("connection refused")}
	mgr := newLazyManager(map[string]sources.SourceConfig{"lazy": cfg})

	if _, err := mgr.ResolveSource(context.Background(), "lazy"); err == nil {
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

	if _, err := mgr.ResolveSource(context.Background(), "lazy"); err != nil {
		t.Fatalf("expected the retry to succeed, got %s", err)
	}
	if got := cfg.callCount(); got != 2 {
		t.Fatalf("expected 2 Initialize calls, got %d", got)
	}
}

func TestResolveSourceUnknownName(t *testing.T) {
	cfg := &countingSourceConfig{}
	mgr := newLazyManager(map[string]sources.SourceConfig{"lazy": cfg})

	if _, err := mgr.ResolveSource(context.Background(), "nonexistent"); err == nil {
		t.Fatal("expected an error for an unconfigured source")
	}
	if got := cfg.callCount(); got != 0 {
		t.Fatalf("expected no Initialize calls, got %d", got)
	}
}

func TestResolveSourceEager(t *testing.T) {
	// Without SetLazySources the manager only serves already-connected sources.
	src := testutils.MockSource{MockSourceConfig: testutils.MockSourceConfig{Name: "eager", Type: "mock"}}
	mgr := primitives.NewPrimitiveManager(map[string]sources.Source{"eager": src}, nil, nil, nil, nil, nil)

	if mgr.LazySources() {
		t.Fatal("expected lazy sources to be off by default")
	}
	got, err := mgr.ResolveSource(context.Background(), "eager")
	if err != nil {
		t.Fatalf("unexpected error resolving a connected source: %s", err)
	}
	if diff := cmp.Diff(got, sources.Source(src)); diff != "" {
		t.Errorf("unexpected source (-want +got):\n%s", diff)
	}
	if _, err := mgr.ResolveSource(context.Background(), "missing"); err == nil {
		t.Fatal("expected an error for a source that was never initialized")
	}
}
