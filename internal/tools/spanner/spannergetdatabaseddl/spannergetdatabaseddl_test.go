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

package spannergetdatabaseddl_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/mcp-toolbox/internal/server"
	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/internal/tools"
	"github.com/googleapis/mcp-toolbox/internal/tools/spanner/spannergetdatabaseddl"
	"github.com/googleapis/mcp-toolbox/internal/util/parameters"
)

func TestParseFromYamlGetDatabaseDdl(t *testing.T) {
	ctx, err := testutils.ContextWithNewLogger()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tcs := []struct {
		desc string
		in   string
		want server.ToolConfigs
	}{
		{
			desc: "basic example",
			in: `
            kind: tool
            name: get_database_ddl
            type: spanner-get-database-ddl
            source: my-instance
            description: some description
            `,
			want: server.ToolConfigs{
				"get_database_ddl": spannergetdatabaseddl.Config{
					ConfigBase: tools.ConfigBase{
						Name:         "get_database_ddl",
						Description:  "some description",
						AuthRequired: []string{},
					},
					Type:   "spanner-get-database-ddl",
					Source: "my-instance",
				},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			_, _, _, got, _, _, err := server.UnmarshalPrimitiveConfig(ctx, testutils.FormatYaml(tc.in))
			if err != nil {
				t.Fatalf("unable to unmarshal: %s", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("incorrect parse: diff %v", diff)
			}
		})
	}
}

type mockSpannerSource struct {
	ddlStatements []string
	err           error
	readOnly      bool
}

func (m mockSpannerSource) GetDatabaseDdl(ctx context.Context, tokenString string) ([]string, error) {
	return m.ddlStatements, m.err
}
func (m mockSpannerSource) UseClientAuthorization() bool   { return false }
func (m mockSpannerSource) IsReadOnly() bool               { return m.readOnly }
func (m mockSpannerSource) SourceType() string             { return "spanner" }
func (m mockSpannerSource) ToConfig() sources.SourceConfig { return nil }

func TestConfig_Initialize(t *testing.T) {
	cfg := spannergetdatabaseddl.Config{
		ConfigBase: tools.ConfigBase{
			Name:        "get_database_ddl",
			Description: "Test DDL retrieval",
		},
		Type:   "spanner-get-database-ddl",
		Source: "test-source",
	}

	tool, err := cfg.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if tool.GetName() != "get_database_ddl" {
		t.Errorf("GetName() = %v, want %v", tool.GetName(), "get_database_ddl")
	}

	// ReadOnly annotations default
	ann := tool.GetAnnotations(nil)
	if ann == nil || ann.ReadOnlyHint == nil || !*ann.ReadOnlyHint {
		t.Errorf("expected ReadOnlyHint=true, got %+v", ann)
	}
	if ann.DestructiveHint != nil && *ann.DestructiveHint {
		t.Errorf("expected DestructiveHint=false, got %+v", ann)
	}
}

func TestTool_ShouldSuppress(t *testing.T) {
	cfg := spannergetdatabaseddl.Config{
		ConfigBase: tools.ConfigBase{Name: "get_database_ddl"},
		Type:       "spanner-get-database-ddl",
		Source:     "test-source",
	}
	tool, err := cfg.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	roSource := mockSpannerSource{readOnly: true}
	rwSource := mockSpannerSource{readOnly: false}

	if tools.ShouldSuppress(context.Background(), tool, roSource) {
		t.Errorf("expected get_database_ddl to NOT be suppressed on read-only source")
	}
	if tools.ShouldSuppress(context.Background(), tool, rwSource) {
		t.Errorf("expected get_database_ddl to NOT be suppressed on read-write source")
	}
}

func TestTool_Invoke(t *testing.T) {
	ctx := context.Background()
	mockSource := mockSpannerSource{
		ddlStatements: []string{
			"CREATE TABLE users (id INT64, name STRING(100)) PRIMARY KEY (id)",
			"CREATE INDEX idx_users_name ON users (name)",
		},
	}

	cfg := spannergetdatabaseddl.Config{
		ConfigBase: tools.ConfigBase{Name: "get_database_ddl"},
		Type:       "spanner-get-database-ddl",
		Source:     "test-source",
	}
	tool, err := cfg.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	resp, err := tool.Invoke(ctx, mockSource, parameters.ParamValues{}, tools.AccessToken(""))
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	respMap, ok := resp.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", resp)
	}

	stmts, ok := respMap["statements"].([]string)
	if !ok {
		t.Fatalf("expected []string statements, got %T", respMap["statements"])
	}

	if len(stmts) != 2 || stmts[0] != mockSource.ddlStatements[0] {
		t.Errorf("unexpected statements: %v", stmts)
	}
}
