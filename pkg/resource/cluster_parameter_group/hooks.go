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
	"fmt"
	"sort"
	"strings"
	"time"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"

	svcapitypes "github.com/aws-controllers-k8s/redshift-controller/apis/v1alpha1"
)

// maxParametersPerCall is the most parameters ModifyClusterParameterGroup and
// ResetClusterParameterGroup accept in a single request.
const maxParametersPerCall = 20

var (
	errParametersModified = fmt.Errorf("parameter overrides modified, requeuing to refresh status")
	// requeueWaitAfterParameterSync re-reads the group after a parameter change
	// so Status.ParameterOverrideStatuses reflects it (rds-controller#298).
	requeueWaitAfterParameterSync = ackrequeue.NeededAfter(errParametersModified, 5*time.Second)
)

// hasUpperCase reports whether name contains upper-case letters.
func hasUpperCase(name *string) bool {
	return name != nil && strings.ToLower(*name) != *name
}

// validateParameterGroupName rejects an upper-case name before create: Redshift
// stores group names in lower case, so such a CR would never sync.
func validateParameterGroupName(name *string) error {
	if !hasUpperCase(name) {
		return nil
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"spec.name %q contains upper-case letters; Redshift stores parameter group names in lower case, so the resource would never sync. spec.name is immutable; delete and recreate the resource with %q",
		*name, strings.ToLower(*name)))
}

// customUpdateClusterParameterGroup replaces the generated single Modify call
// with parameter-sync semantics, as rds-controller DBParameterGroup does
// (community#869): parameters removed from the spec are reset with
// ResetClusterParameterGroup, changes are sent in batches of at most 20, and
// Modify is skipped when no parameter changed. Only Tags and
// ParameterOverrides are mutable.
func (rm *resourceManager) customUpdateClusterParameterGroup(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) (updated *resource, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.customUpdateClusterParameterGroup")
	defer func() {
		exit(err)
	}()

	if delta.DifferentAt("Spec.Tags") {
		if err = rm.syncTags(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	updated = &resource{desired.ko.DeepCopy()}
	updated.SetStatus(latest)
	if delta.DifferentAt("Spec.ParameterOverrides") {
		if err = rm.syncParameters(ctx, desired, latest); err != nil {
			return nil, err
		}
		return updated, requeueWaitAfterParameterSync
	}
	return updated, nil
}

// syncTags applies the tag diff between desired and latest with
// CreateTags/DeleteTags. DescribeClusterParameterGroups returns no ARN, so the
// ARN is built from the resource metadata and the group name.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTags")
	defer func() {
		exit(err)
	}()

	var arn string
	if desired.ko.Status.ACKResourceMetadata != nil && desired.ko.Status.ACKResourceMetadata.ARN != nil {
		arn = string(*desired.ko.Status.ACKResourceMetadata.ARN)
	} else {
		md := desired.ko.Status.ACKResourceMetadata
		if md == nil {
			md = latest.ko.Status.ACKResourceMetadata
		}
		if md == nil || md.OwnerAccountID == nil || md.Region == nil || md.Partition == nil {
			return fmt.Errorf("cannot sync tags: resource metadata is not available")
		}
		if desired.ko.Spec.Name == nil {
			return fmt.Errorf("cannot sync tags: resource name is not available")
		}
		arn = fmt.Sprintf("arn:%s:redshift:%s:%s:parametergroup:%s",
			*md.Partition, *md.Region, *md.OwnerAccountID, *desired.ko.Spec.Name)
	}

	desiredTags, _ := convertToOrderedACKTags(desired.ko.Spec.Tags)
	latestTags, _ := convertToOrderedACKTags(latest.ko.Spec.Tags)

	var tagsToAdd []svcsdktypes.Tag
	for k, v := range desiredTags {
		if latestVal, exists := latestTags[k]; !exists || latestVal != v {
			tagKey, tagVal := k, v
			tagsToAdd = append(tagsToAdd, svcsdktypes.Tag{Key: &tagKey, Value: &tagVal})
		}
	}
	var tagKeysToRemove []string
	for k := range latestTags {
		if _, exists := desiredTags[k]; !exists {
			tagKeysToRemove = append(tagKeysToRemove, k)
		}
	}

	if len(tagKeysToRemove) > 0 {
		_, err = rm.sdkapi.DeleteTags(ctx, &svcsdk.DeleteTagsInput{
			ResourceName: &arn,
			TagKeys:      tagKeysToRemove,
		})
		rm.metrics.RecordAPICall("UPDATE", "DeleteTags", err)
		if err != nil {
			return err
		}
	}
	if len(tagsToAdd) > 0 {
		_, err = rm.sdkapi.CreateTags(ctx, &svcsdk.CreateTagsInput{
			ResourceName: &arn,
			Tags:         tagsToAdd,
		})
		rm.metrics.RecordAPICall("UPDATE", "CreateTags", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// getParameterOverrides reads every page of the group's parameters and
// returns the overrides and their statuses, as filtered by filterParameters.
func (rm *resourceManager) getParameterOverrides(
	ctx context.Context,
	groupName *string,
	declared map[string]*string,
) (overrides map[string]*string, statuses []*svcapitypes.Parameter, err error) {
	var params []svcsdktypes.Parameter
	input := &svcsdk.DescribeClusterParametersInput{ParameterGroupName: groupName}
	for {
		resp, err := rm.sdkapi.DescribeClusterParameters(ctx, input)
		rm.metrics.RecordAPICall("READ_MANY", "DescribeClusterParameters", err)
		if err != nil {
			if awsErr, ok := ackerr.AWSError(err); ok && awsErr.ErrorCode() == "ClusterParameterGroupNotFound" {
				return nil, nil, ackerr.NotFound
			}
			return nil, nil, err
		}
		params = append(params, resp.Parameters...)
		if resp.Marker == nil || *resp.Marker == "" {
			break
		}
		input.Marker = resp.Marker
	}
	overrides, statuses = filterParameters(params, declared)
	return overrides, statuses, nil
}

// filterParameters keeps the parameters the user changed (Source "user") plus
// any the CR declares, and returns them as overrides and as statuses sorted by
// name. Declared names are kept regardless of Source so a declared value equal
// to the engine default still compares equal and the delta closes.
func filterParameters(
	params []svcsdktypes.Parameter,
	declared map[string]*string,
) (map[string]*string, []*svcapitypes.Parameter) {
	overrides := map[string]*string{}
	var statuses []*svcapitypes.Parameter
	for _, p := range params {
		if p.ParameterName == nil || p.ParameterValue == nil {
			continue
		}
		_, isDeclared := declared[*p.ParameterName]
		if !isDeclared && aws.ToString(p.Source) != "user" {
			continue
		}
		overrides[*p.ParameterName] = p.ParameterValue
		var applyType *string
		if p.ApplyType != "" {
			applyType = aws.String(string(p.ApplyType))
		}
		statuses = append(statuses, &svcapitypes.Parameter{
			ParameterName:  p.ParameterName,
			ParameterValue: p.ParameterValue,
			ApplyType:      applyType,
			IsModifiable:   p.IsModifiable,
			Source:         p.Source,
		})
	}
	if len(overrides) == 0 {
		return nil, nil
	}
	sort.Slice(statuses, func(i, j int) bool {
		return *statuses[i].ParameterName < *statuses[j].ParameterName
	})
	return overrides, statuses
}

// syncParameters resets parameters dropped from the spec, then modifies the
// added or changed ones, in batches of at most 20. Reset runs first so it
// cannot undo a value set in the same pass. If a call fails partway (e.g.
// InvalidClusterParameterGroupState), the next reconcile re-reads the group
// and recomputes the diff, so a partial application heals itself.
func (rm *resourceManager) syncParameters(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncParameters")
	defer func() {
		exit(err)
	}()

	var latestParams map[string]*string
	if latest != nil {
		latestParams = latest.ko.Spec.ParameterOverrides
	}
	toReset, toModify := diffParameters(desired.ko.Spec.ParameterOverrides, latestParams)
	groupName := desired.ko.Spec.Name

	for _, names := range chunk(toReset, maxParametersPerCall) {
		_, err = rm.sdkapi.ResetClusterParameterGroup(ctx, newResetParametersInput(groupName, names))
		rm.metrics.RecordAPICall("UPDATE", "ResetClusterParameterGroup", err)
		if err != nil {
			return err
		}
	}

	modifyNames := make([]string, 0, len(toModify))
	for name := range toModify {
		modifyNames = append(modifyNames, name)
	}
	sort.Strings(modifyNames)
	for _, names := range chunk(modifyNames, maxParametersPerCall) {
		_, err = rm.sdkapi.ModifyClusterParameterGroup(ctx, newModifyParametersInput(groupName, names, toModify))
		rm.metrics.RecordAPICall("UPDATE", "ModifyClusterParameterGroup", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// diffParameters returns the names present in latest but absent from desired
// (to reset, sorted) and the desired values that are new or changed (to
// modify). Entries with a nil value are skipped.
func diffParameters(
	desired map[string]*string,
	latest map[string]*string,
) (toReset []string, toModify map[string]string) {
	toModify = map[string]string{}
	for name, value := range desired {
		if value == nil {
			continue
		}
		if cur, ok := latest[name]; !ok || cur == nil || *cur != *value {
			toModify[name] = *value
		}
	}
	for name, value := range latest {
		if value == nil {
			continue
		}
		if _, ok := desired[name]; !ok {
			toReset = append(toReset, name)
		}
	}
	sort.Strings(toReset)
	return toReset, toModify
}

// chunk splits items into consecutive slices of at most size elements.
func chunk[T any](items []T, size int) [][]T {
	var out [][]T
	for start := 0; start < len(items); start += size {
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		out = append(out, items[start:end])
	}
	return out
}

// newResetParametersInput resets only the named parameters. ResetAllParameters
// defaults to true in the API, so it must be sent as an explicit false.
func newResetParametersInput(
	groupName *string,
	names []string,
) *svcsdk.ResetClusterParameterGroupInput {
	params := make([]svcsdktypes.Parameter, 0, len(names))
	for _, name := range names {
		params = append(params, svcsdktypes.Parameter{ParameterName: aws.String(name)})
	}
	return &svcsdk.ResetClusterParameterGroupInput{
		ParameterGroupName: groupName,
		ResetAllParameters: aws.Bool(false),
		Parameters:         params,
	}
}

// newModifyParametersInput sets the named parameters to their values.
func newModifyParametersInput(
	groupName *string,
	names []string,
	values map[string]string,
) *svcsdk.ModifyClusterParameterGroupInput {
	params := make([]svcsdktypes.Parameter, 0, len(names))
	for _, name := range names {
		params = append(params, svcsdktypes.Parameter{
			ParameterName:  aws.String(name),
			ParameterValue: aws.String(values[name]),
		})
	}
	return &svcsdk.ModifyClusterParameterGroupInput{
		ParameterGroupName: groupName,
		Parameters:         params,
	}
}
