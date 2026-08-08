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

package util

import (
	"context"

	"github.com/googleapis/mcp-toolbox/internal/util"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecordToolExecutionFailure records a tool call that failed before the tool
// itself ran, on the same metric a failing Invoke reports to.
//
// A source that cannot be connected is returned to the agent as a tool result
// with IsError set, not as a JSON-RPC error, so nothing upstream marks the
// request as failed. Without this the whole class of failure is invisible to
// the metric operators watch for broken tools.
func RecordToolExecutionFailure(ctx context.Context, toolName string, duration float64, err error) {
	instrumentation, instrumentationErr := util.InstrumentationFromContext(ctx)
	if instrumentationErr != nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("gen_ai.tool.name", toolName),
		attribute.String("error.type", err.Error()),
	}
	if genAIAttrs := util.GenAIMetricAttrsFromContext(ctx); genAIAttrs != nil {
		if genAIAttrs.NetworkProtocolName != "" {
			attrs = append(attrs, attribute.String("network.protocol.name", genAIAttrs.NetworkProtocolName))
		}
		if genAIAttrs.NetworkProtocolVersion != "" {
			attrs = append(attrs, attribute.String("network.protocol.version", genAIAttrs.NetworkProtocolVersion))
		}
	}
	instrumentation.ToolExecutionDuration.Record(ctx, duration, metric.WithAttributes(attrs...))
}
