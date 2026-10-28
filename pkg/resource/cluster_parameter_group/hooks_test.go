// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package cluster_parameter_group

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"

	svcapitypes "github.com/aws-controllers-k8s/redshift-controller/apis/v1alpha1"
)

func TestDiffParameters(t *testing.T) {
	tests := []struct {
		name       string
		desired    map[string]*string
		latest     map[string]*string
		wantReset  []string
		wantModify map[string]string
	}{
		{
			name:       "add",
			desired:    map[string]*string{"a": aws.String("1"), "b": aws.String("2")},
			latest:     map[string]*string{"a": aws.String("1")},
			wantModify: map[string]string{"b": "2"},
		},
		{
			name:       "change",
			desired:    map[string]*string{"a": aws.String("2")},
			latest:     map[string]*string{"a": aws.String("1")},
			wantModify: map[string]string{"a": "2"},
		},
		{
			name:       "remove",
			desired:    map[string]*string{"a": aws.String("1")},
			latest:     map[string]*string{"a": aws.String("1"), "c": aws.String("3"), "b": aws.String("2")},
			wantReset:  []string{"b", "c"},
			wantModify: map[string]string{},
		},
		{
			name:       "unchanged",
			desired:    map[string]*string{"a": aws.String("1")},
			latest:     map[string]*string{"a": aws.String("1")},
			wantModify: map[string]string{},
		},
		{
			name:       "nil latest (create path)",
			desired:    map[string]*string{"a": aws.String("1"), "b": aws.String("2")},
			latest:     nil,
			wantModify: map[string]string{"a": "1", "b": "2"},
		},
		{
			name:       "empty desired resets every known name",
			desired:    nil,
			latest:     map[string]*string{"a": aws.String("1"), "b": aws.String("2")},
			wantReset:  []string{"a", "b"},
			wantModify: map[string]string{},
		},
		{
			name:       "nil value skipped",
			desired:    map[string]*string{"a": nil, "b": aws.String("2")},
			latest:     map[string]*string{"c": nil},
			wantModify: map[string]string{"b": "2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReset, gotModify := diffParameters(tt.desired, tt.latest)
			if len(gotReset) != 0 || len(tt.wantReset) != 0 {
				if !reflect.DeepEqual(gotReset, tt.wantReset) {
					t.Errorf("toReset = %v, want %v", gotReset, tt.wantReset)
				}
			}
			if !reflect.DeepEqual(gotModify, tt.wantModify) {
				t.Errorf("toModify = %v, want %v", gotModify, tt.wantModify)
			}
		})
	}
}

func TestChunk(t *testing.T) {
	for _, tt := range []struct {
		n          int
		wantChunks int
	}{{0, 0}, {1, 1}, {20, 1}, {21, 2}, {41, 3}} {
		t.Run(fmt.Sprintf("%d items", tt.n), func(t *testing.T) {
			items := make([]int, tt.n)
			for i := range items {
				items[i] = i
			}
			chunks := chunk(items, maxParametersPerCall)
			if len(chunks) != tt.wantChunks {
				t.Fatalf("got %d chunks, want %d", len(chunks), tt.wantChunks)
			}
			// Every item appears exactly once, in order, and no chunk is oversized.
			next := 0
			for _, c := range chunks {
				if len(c) == 0 || len(c) > maxParametersPerCall {
					t.Fatalf("chunk size %d out of range", len(c))
				}
				for _, v := range c {
					if v != next {
						t.Fatalf("got item %d, want %d", v, next)
					}
					next++
				}
			}
			if next != tt.n {
				t.Fatalf("saw %d items, want %d", next, tt.n)
			}
		})
	}
}

func TestNewResetParametersInput(t *testing.T) {
	input := newResetParametersInput(aws.String("grp"), []string{"a", "b"})
	if input.ResetAllParameters == nil || *input.ResetAllParameters {
		t.Fatalf("ResetAllParameters must be an explicit false, got %v", input.ResetAllParameters)
	}
	if aws.ToString(input.ParameterGroupName) != "grp" {
		t.Errorf("ParameterGroupName = %q", aws.ToString(input.ParameterGroupName))
	}
	if len(input.Parameters) != 2 ||
		aws.ToString(input.Parameters[0].ParameterName) != "a" ||
		aws.ToString(input.Parameters[1].ParameterName) != "b" {
		t.Errorf("unexpected Parameters %+v", input.Parameters)
	}
	for _, p := range input.Parameters {
		if p.ParameterValue != nil {
			t.Errorf("reset of %s must not carry a value", aws.ToString(p.ParameterName))
		}
	}
}

func TestNewModifyParametersInput(t *testing.T) {
	values := map[string]string{"a": "1", "b": "2", "c": "3"}
	input := newModifyParametersInput(aws.String("grp"), []string{"a", "c"}, values)
	if aws.ToString(input.ParameterGroupName) != "grp" {
		t.Errorf("ParameterGroupName = %q", aws.ToString(input.ParameterGroupName))
	}
	got := map[string]string{}
	for _, p := range input.Parameters {
		got[aws.ToString(p.ParameterName)] = aws.ToString(p.ParameterValue)
	}
	if want := map[string]string{"a": "1", "c": "3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Parameters = %v, want %v", got, want)
	}
}

func TestHasUpperCase(t *testing.T) {
	for _, tc := range []struct {
		name *string
		want bool
	}{
		{nil, false},
		{aws.String(""), false},
		{aws.String("abc-1"), false},
		{aws.String("Abc"), true},
		{aws.String("ABC"), true},
		{aws.String("abc-D"), true},
	} {
		if got := hasUpperCase(tc.name); got != tc.want {
			t.Errorf("hasUpperCase(%q) = %v, want %v", aws.ToString(tc.name), got, tc.want)
		}
	}
}

func TestValidateParameterGroupName(t *testing.T) {
	for _, name := range []*string{nil, aws.String("abc-1")} {
		if err := validateParameterGroupName(name); err != nil {
			t.Errorf("validateParameterGroupName(%q) = %v, want nil", aws.ToString(name), err)
		}
	}
	err := validateParameterGroupName(aws.String("My-Group"))
	var terminal *ackerr.TerminalError
	if !errors.As(err, &terminal) {
		t.Fatalf("got %v, want a *ackerr.TerminalError", err)
	}
	for _, want := range []string{"spec.name is immutable", `"my-group"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	}
}

// TestUpperCaseNameGuards exercises the generated wiring of both hooks: each
// must return before any AWS call, so a zero-value resourceManager suffices.
func TestUpperCaseNameGuards(t *testing.T) {
	res := func(name string) *resource {
		return &resource{&svcapitypes.ClusterParameterGroup{
			Spec: svcapitypes.ClusterParameterGroupSpec{Name: aws.String(name)},
		}}
	}
	rm := &resourceManager{}
	ctx := context.Background()

	if _, err := rm.sdkFind(ctx, res("Foo")); err != ackerr.NotFound {
		t.Errorf("sdkFind = %v, want ackerr.NotFound", err)
	}
	_, err := rm.sdkCreate(ctx, res("Foo"))
	var terminal *ackerr.TerminalError
	if !errors.As(err, &terminal) {
		t.Errorf("sdkCreate = %v, want a *ackerr.TerminalError", err)
	}
}

func TestFilterParameters(t *testing.T) {
	params := []svcsdktypes.Parameter{
		{ParameterName: aws.String("z_user"), ParameterValue: aws.String("1"), Source: aws.String("user"),
			ApplyType: svcsdktypes.ParameterApplyTypeDynamic, IsModifiable: aws.Bool(true)},
		{ParameterName: aws.String("a_declared_default"), ParameterValue: aws.String("2"), Source: aws.String("engine-default"),
			ApplyType: svcsdktypes.ParameterApplyTypeStatic, IsModifiable: aws.Bool(true)},
		{ParameterName: aws.String("m_undeclared_default"), ParameterValue: aws.String("3"), Source: aws.String("engine-default")},
		{ParameterName: aws.String("n_nil_value"), Source: aws.String("user")},
		{ParameterName: aws.String("b_no_apply_type"), ParameterValue: aws.String("4"), Source: aws.String("user")},
	}
	declared := map[string]*string{"a_declared_default": aws.String("2"), "n_nil_value": aws.String("x")}

	overrides, statuses := filterParameters(params, declared)
	wantOverrides := map[string]*string{
		"z_user":             aws.String("1"),
		"a_declared_default": aws.String("2"),
		"b_no_apply_type":    aws.String("4"),
	}
	if !reflect.DeepEqual(overrides, wantOverrides) {
		t.Errorf("overrides = %v, want %v", overrides, wantOverrides)
	}
	wantStatuses := []*svcapitypes.Parameter{
		{ParameterName: aws.String("a_declared_default"), ParameterValue: aws.String("2"), Source: aws.String("engine-default"),
			ApplyType: aws.String("static"), IsModifiable: aws.Bool(true)},
		{ParameterName: aws.String("b_no_apply_type"), ParameterValue: aws.String("4"), Source: aws.String("user")},
		{ParameterName: aws.String("z_user"), ParameterValue: aws.String("1"), Source: aws.String("user"),
			ApplyType: aws.String("dynamic"), IsModifiable: aws.Bool(true)},
	}
	if !reflect.DeepEqual(statuses, wantStatuses) {
		t.Errorf("statuses = %+v, want %+v", statuses, wantStatuses)
	}

	overrides, statuses = filterParameters(params[2:3], nil)
	if overrides != nil || statuses != nil {
		t.Errorf("nothing kept: got %v, %v, want nil, nil", overrides, statuses)
	}
}

func TestRequeueAfterParameterSync(t *testing.T) {
	var requeue *ackrequeue.RequeueNeededAfter
	if !errors.As(requeueWaitAfterParameterSync, &requeue) {
		t.Fatalf("not a *ackrequeue.RequeueNeededAfter")
	}
	if requeue.Duration() != 5*time.Second {
		t.Errorf("Duration() = %v, want 5s", requeue.Duration())
	}
	if _, ok := ackerr.AWSError(requeueWaitAfterParameterSync); ok {
		t.Errorf("classified as an AWS error")
	}
	var terminal *ackerr.TerminalError
	if errors.As(requeueWaitAfterParameterSync, &terminal) {
		t.Errorf("classified as terminal")
	}
}
