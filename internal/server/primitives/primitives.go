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

package primitives

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/googleapis/mcp-toolbox/internal/auth"
	"github.com/googleapis/mcp-toolbox/internal/embeddingmodels"
	"github.com/googleapis/mcp-toolbox/internal/group"
	"github.com/googleapis/mcp-toolbox/internal/prompts"
	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/tools"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"
)

// PrimitiveManager contains available resources for the server. Should be initialized with NewPrimitiveManager().
// groups is the source of truth for named collections; toolset views (manifests)
// are derived from the group on demand by the callers that render them.
type PrimitiveManager struct {
	mu              sync.RWMutex
	sources         map[string]sources.Source
	authServices    map[string]auth.AuthService
	embeddingModels map[string]embeddingmodels.EmbeddingModel
	tools           map[string]tools.Tool
	prompts         map[string]prompts.Prompt
	groups          map[string]group.Group

	// Lazy source initialization. When enabled, sources are absent from the
	// sources map until ResolveSource connects one on first use.
	lazy          bool
	sourceConfigs map[string]sources.SourceConfig
	tracer        trace.Tracer
	initGroup     singleflight.Group
}

func NewPrimitiveManager(
	sourcesMap map[string]sources.Source,
	authServicesMap map[string]auth.AuthService,
	embeddingModelsMap map[string]embeddingmodels.EmbeddingModel,
	toolsMap map[string]tools.Tool,
	promptsMap map[string]prompts.Prompt,
	groupsMap map[string]group.Group,

) *PrimitiveManager {
	primitiveMgr := &PrimitiveManager{
		mu:              sync.RWMutex{},
		sources:         sourcesMap,
		authServices:    authServicesMap,
		embeddingModels: embeddingModelsMap,
		tools:           toolsMap,
		prompts:         promptsMap,
		groups:          groupsMap,
	}

	return primitiveMgr
}

// GetSource returns a source only if it is already connected. It never blocks,
// so listing paths can use it and fall back to a tool's static manifest when a
// lazily-initialized source has not been reached yet. Invocation paths want
// ResolveSource instead.
func (r *PrimitiveManager) GetSource(sourceName string) (sources.Source, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	source, ok := r.sources[sourceName]
	return source, ok
}

// SetLazySources enables deferred source initialization: sources stay absent
// from the sources map until ResolveSource connects them on first use.
func (r *PrimitiveManager) SetLazySources(sourceConfigs map[string]sources.SourceConfig, tracer trace.Tracer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sources == nil {
		r.sources = make(map[string]sources.Source, len(sourceConfigs))
	}
	r.lazy = true
	r.sourceConfigs = sourceConfigs
	r.tracer = tracer
}

// LazySources reports whether sources are connected on first use.
func (r *PrimitiveManager) LazySources() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lazy
}

// ResolveSource returns the named source, connecting it first if lazy source
// initialization is enabled and it has not been reached yet. Unlike GetSource
// it blocks on I/O and can fail, so it belongs on invocation paths.
func (r *PrimitiveManager) ResolveSource(ctx context.Context, sourceName string) (sources.Source, error) {
	r.mu.RLock()
	source, connected := r.sources[sourceName]
	sourceConfig, configured := r.sourceConfigs[sourceName]
	tracer := r.tracer
	r.mu.RUnlock()

	if connected {
		return source, nil
	}
	if !configured {
		return nil, fmt.Errorf("unable to retrieve source %q", sourceName)
	}

	// singleflight collapses the connection attempts that race on a cold
	// source. A failure is deliberately not cached, so a source that was down
	// or misconfigured starts working on a later call without a restart.
	resolved, err, _ := r.initGroup.Do(sourceName, func() (any, error) {
		r.mu.RLock()
		source, connected := r.sources[sourceName]
		r.mu.RUnlock()
		if connected {
			return source, nil
		}

		childCtx, span := tracer.Start(
			ctx,
			"toolbox/server/source/init",
			trace.WithAttributes(attribute.String("source_type", sourceConfig.SourceConfigType())),
			trace.WithAttributes(attribute.String("source_name", sourceName)),
		)
		defer span.End()

		source, err := sourceConfig.Initialize(childCtx, tracer)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("unable to initialize source %q: %w", sourceName, err)
		}

		r.mu.Lock()
		r.sources[sourceName] = source
		r.mu.Unlock()
		return source, nil
	})
	if err != nil {
		return nil, err
	}
	return resolved.(sources.Source), nil
}

func (r *PrimitiveManager) GetAuthService(authServiceName string) (auth.AuthService, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	authService, ok := r.authServices[authServiceName]
	return authService, ok
}

func (r *PrimitiveManager) GetEmbeddingModel(embeddingModelName string) (embeddingmodels.EmbeddingModel, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	model, ok := r.embeddingModels[embeddingModelName]
	return model, ok
}

func (r *PrimitiveManager) GetTool(toolName string) (tools.Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tool, ok := r.tools[toolName]
	return tool, ok
}

func (r *PrimitiveManager) GetPrompt(promptName string) (prompts.Prompt, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	prompt, ok := r.prompts[promptName]
	return prompt, ok
}

// GetGroup returns the group of the given name.
func (r *PrimitiveManager) GetGroup(groupName string) (group.Group, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[groupName]
	return g, ok
}

// SetPrimitives replaces every primitive map. Callers using lazy sources must
// follow with SetLazySources to install the reloaded source configs, since the
// previous ones no longer describe the sources map they just swapped in.
func (r *PrimitiveManager) SetPrimitives(sourcesMap map[string]sources.Source, authServicesMap map[string]auth.AuthService, embeddingModelsMap map[string]embeddingmodels.EmbeddingModel, toolsMap map[string]tools.Tool, promptsMap map[string]prompts.Prompt, groupsMap map[string]group.Group) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources = sourcesMap
	r.authServices = authServicesMap
	r.embeddingModels = embeddingModelsMap
	r.tools = toolsMap
	r.prompts = promptsMap
	r.groups = groupsMap
}

func (r *PrimitiveManager) GetAuthServiceMap() map[string]auth.AuthService {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copiedMap := make(map[string]auth.AuthService, len(r.authServices))
	for k, v := range r.authServices {
		copiedMap[k] = v
	}
	return copiedMap
}

// GroupsList returns a copy of the groups list sorted alphabetically by name
func (r *PrimitiveManager) GroupsList() []group.Group {
	r.mu.RLock()
	defer r.mu.RUnlock()
	groupsList := make([]group.Group, 0, len(r.groups))
	for k, g := range r.groups {
		if k == "" {
			continue
		}
		groupsList = append(groupsList, g)
	}

	slices.SortFunc(groupsList, func(a, b group.Group) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return groupsList
}
