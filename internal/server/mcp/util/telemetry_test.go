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

package util_test

import (
	"context"
	"errors"
	"testing"

	mcputil "github.com/googleapis/mcp-toolbox/internal/server/mcp/util"
	"github.com/googleapis/mcp-toolbox/internal/telemetry"
	"github.com/googleapis/mcp-toolbox/internal/util"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const toolExecutionDurationName = "toolbox.tool.execution.duration"

func TestRecordToolExecutionFailureWithoutInstrumentation(t *testing.T) {
	// A context with no instrumentation reaches this on the stdio transport,
	// where a panic would take the whole process down rather than fail a call.
	mcputil.RecordToolExecutionFailure(context.Background(), "my_tool", 0.5, errors.New("connection refused"))
}

func TestRecordToolExecutionFailureRecordsErrorType(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	defer otel.SetMeterProvider(previous)

	instrumentation, err := telemetry.CreateTelemetryInstrumentation("test")
	if err != nil {
		t.Fatalf("unable to create instrumentation: %s", err)
	}
	ctx := util.WithInstrumentation(context.Background(), instrumentation)

	mcputil.RecordToolExecutionFailure(ctx, "my_tool", 0.5, errors.New("connection refused"))

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatalf("unable to collect metrics: %s", err)
	}

	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != toolExecutionDurationName {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("expected a float64 histogram, got %T", m.Data)
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("expected 1 data point, got %d", len(hist.DataPoints))
			}
			attrs := hist.DataPoints[0].Attributes
			errType, ok := attrs.Value("error.type")
			if !ok {
				t.Fatalf("expected an error.type attribute, got %v", attrs.ToSlice())
			}
			if errType.AsString() != "connection refused" {
				t.Errorf("unexpected error.type: %q", errType.AsString())
			}
			toolName, ok := attrs.Value("gen_ai.tool.name")
			if !ok || toolName.AsString() != "my_tool" {
				t.Errorf("expected gen_ai.tool.name=my_tool, got %v", attrs.ToSlice())
			}
			return
		}
	}
	t.Fatalf("no %s metric was recorded", toolExecutionDurationName)
}
