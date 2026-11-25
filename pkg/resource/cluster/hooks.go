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

package cluster

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	smithy "github.com/aws/smithy-go"

	svcapitypes "github.com/aws-controllers-k8s/redshift-controller/apis/v1alpha1"
)

const (
	// AnnotationSkipFinalSnapshot sets DeleteCluster.SkipFinalClusterSnapshot.
	AnnotationSkipFinalSnapshot = "redshift.services.k8s.aws/skip-final-snapshot"
	// AnnotationFinalSnapshotIdentifier sets
	// DeleteCluster.FinalClusterSnapshotIdentifier.
	AnnotationFinalSnapshotIdentifier = "redshift.services.k8s.aws/final-snapshot-identifier"
	// AnnotationFinalSnapshotRetentionPeriod sets
	// DeleteCluster.FinalClusterSnapshotRetentionPeriod.
	AnnotationFinalSnapshotRetentionPeriod = "redshift.services.k8s.aws/final-snapshot-retention-period"

	// requeueAfterSecondaryModify is how long to wait after a side-API modify
	// (IAM roles, snapshot schedule) before the next pass, so the cluster can
	// leave the state that makes ModifyCluster return InvalidClusterState.
	requeueAfterSecondaryModify = 15 * time.Second

	clusterStatusAvailable = "available"
	iamRoleApplyInSync     = "in-sync"
)

// clusterARN returns the ARN of the cluster with the given identifier.
// DescribeClusters returns no cluster ARN (ClusterNamespaceArn is the
// namespace ARN), so it is built from the manager's partition, region and
// account.
func (rm *resourceManager) clusterARN(identifier string) string {
	return fmt.Sprintf("arn:%s:redshift:%s:%s:cluster:%s",
		rm.awsPartition, rm.awsRegion, rm.awsAccountID, identifier)
}

// parseStatusBool maps the string forms Describe uses for boolean settings
// to a bool. ok is false for anything unrecognised so callers keep the
// declared value instead of manufacturing a delta.
func parseStatusBool(s string) (value bool, ok bool) {
	switch strings.ToLower(s) {
	case "true", "enabled":
		return true, true
	case "false", "disabled":
		return false, true
	}
	return false, false
}

// setObservedSpecFields copies into ko.Spec the values Describe reports in a
// different shape than the Create/Modify input. It only touches fields the
// user declared (non-nil in ko.Spec, which still holds the desired value),
// and leaves the declared value when the observed one is missing or unknown.
func setObservedSpecFields(ko *svcapitypes.Cluster, observed *svcsdktypes.Cluster) {
	spec := &ko.Spec

	if spec.ClusterParameterGroupName != nil {
		for _, pg := range observed.ClusterParameterGroups {
			if pg.ParameterGroupName != nil {
				spec.ClusterParameterGroupName = pg.ParameterGroupName
				break
			}
		}
	}
	if spec.ElasticIP != nil && observed.ElasticIpStatus != nil && observed.ElasticIpStatus.ElasticIp != nil {
		spec.ElasticIP = observed.ElasticIpStatus.ElasticIp
	}
	if observed.HsmStatus != nil {
		if spec.HsmClientCertificateIdentifier != nil && observed.HsmStatus.HsmClientCertificateIdentifier != nil {
			spec.HsmClientCertificateIdentifier = observed.HsmStatus.HsmClientCertificateIdentifier
		}
		if spec.HsmConfigurationIdentifier != nil && observed.HsmStatus.HsmConfigurationIdentifier != nil {
			spec.HsmConfigurationIdentifier = observed.HsmStatus.HsmConfigurationIdentifier
		}
	}
	if spec.Port != nil && observed.Endpoint != nil && observed.Endpoint.Port != nil {
		port := int64(*observed.Endpoint.Port)
		spec.Port = &port
	}
	if spec.Type != nil && observed.NumberOfNodes != nil {
		if *observed.NumberOfNodes == 1 {
			spec.Type = aws.String("single-node")
		} else if *observed.NumberOfNodes > 1 {
			spec.Type = aws.String("multi-node")
		}
	}
	if spec.ManageMasterPassword != nil {
		managed := observed.MasterPasswordSecretArn != nil
		spec.ManageMasterPassword = &managed
	}
	for _, f := range []struct {
		spec     **bool
		observed *string
	}{
		{&spec.MultiAZ, observed.MultiAZ},
		{&spec.ExtraComputeForAutomaticOptimization, observed.ExtraComputeForAutomaticOptimization},
		{&spec.AvailabilityZoneRelocation, observed.AvailabilityZoneRelocationStatus},
	} {
		if *f.spec == nil || f.observed == nil {
			continue
		}
		if v, ok := parseStatusBool(*f.observed); ok {
			*f.spec = &v
		}
	}
}

// observedCluster returns the DescribeClusters entry for identifier, or nil
// when none matches, so the read hook never copies another cluster's values.
func observedCluster(clusters []svcsdktypes.Cluster, identifier *string) *svcsdktypes.Cluster {
	for i := range clusters {
		if identifier != nil && clusters[i].ClusterIdentifier != nil && *clusters[i].ClusterIdentifier == *identifier {
			return &clusters[i]
		}
	}
	return nil
}

// kmsKeyIDOf returns the key ID part of a KMS key ARN
// (arn:...:key/<id>), or v unchanged when it is not a key ARN.
func kmsKeyIDOf(v string) string {
	if i := strings.LastIndex(v, ":key/"); i >= 0 {
		return v[i+len(":key/"):]
	}
	return v
}

// keepEquivalentKMSKeyIDs keeps the declared KMS key when AWS reports the same
// key in the other form (key ID vs key ARN). Without it the delta never closes
// and every reconcile sends ModifyCluster with KmsKeyId, which re-encrypts the
// cluster. Alias forms (alias/..., alias ARNs) are not matched.
func keepEquivalentKMSKeyIDs(latest, desired *svcapitypes.ClusterSpec) {
	for _, f := range []struct {
		latest  **string
		desired *string
	}{
		{&latest.KMSKeyID, desired.KMSKeyID},
		{&latest.MasterPasswordSecretKMSKeyID, desired.MasterPasswordSecretKMSKeyID},
	} {
		if *f.latest == nil || f.desired == nil || **f.latest == *f.desired {
			continue
		}
		if kmsKeyIDOf(**f.latest) == kmsKeyIDOf(*f.desired) {
			*f.latest = aws.String(*f.desired)
		}
	}
}

// applyResizeCoupling makes a user-declared NodeType, NumberOfNodes or Type
// change travel with its partner members: ModifyCluster requires a new node
// count to come with the node type. A partner the user did not declare is taken
// from latest. A late-initialized NumberOfNodes that is nil in desired never
// triggers this, so it cannot turn an otherwise empty request into a resize.
func applyResizeCoupling(
	input *svcsdk.ModifyClusterInput,
	desired, latest *svcapitypes.Cluster,
	delta *ackcompare.Delta,
) error {
	if !((delta.DifferentAt("Spec.NodeType") && desired.Spec.NodeType != nil) ||
		(delta.DifferentAt("Spec.NumberOfNodes") && desired.Spec.NumberOfNodes != nil) ||
		(delta.DifferentAt("Spec.Type") && desired.Spec.Type != nil)) {
		return nil
	}
	nodeType := desired.Spec.NodeType
	if nodeType == nil {
		nodeType = latest.Spec.NodeType
	}
	nodes := desired.Spec.NumberOfNodes
	if nodes == nil {
		nodes = latest.Spec.NumberOfNodes
	}
	input.NodeType = nodeType
	if nodes != nil {
		if *nodes > math.MaxInt32 || *nodes < math.MinInt32 {
			return fmt.Errorf("error: field NumberOfNodes is of type int32")
		}
		n := int32(*nodes)
		input.NumberOfNodes = &n
	}
	if desired.Spec.Type != nil {
		input.ClusterType = desired.Spec.Type
	}
	return nil
}

// modifyClusterInputIsEmpty reports whether the request carries nothing but
// the cluster identifier. AWS rejects such a ModifyCluster call, which happens
// when the only delta is a nil-vs-assigned late-initialized field (e.g.
// AvailabilityZone).
func modifyClusterInputIsEmpty(input *svcsdk.ModifyClusterInput) bool {
	return reflect.DeepEqual(*input, svcsdk.ModifyClusterInput{ClusterIdentifier: input.ClusterIdentifier})
}

// setDeleteOptions fills the DeleteCluster options from annotations. Nothing
// is defaulted: without the annotations AWS requires a final snapshot
// identifier, so delete fails (retryably) until the user adds one; see
// deleteOptionsHint for the error the user then sees.
func setDeleteOptions(annotations map[string]string, input *svcsdk.DeleteClusterInput) error {
	if v, ok := annotations[AnnotationSkipFinalSnapshot]; ok {
		skip, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid value %q for annotation %s: expected true or false", v, AnnotationSkipFinalSnapshot)
		}
		input.SkipFinalClusterSnapshot = &skip
	}
	if v, ok := annotations[AnnotationFinalSnapshotIdentifier]; ok && v != "" {
		input.FinalClusterSnapshotIdentifier = &v
	}
	if v, ok := annotations[AnnotationFinalSnapshotRetentionPeriod]; ok {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid value %q for annotation %s: expected an integer", v, AnnotationFinalSnapshotRetentionPeriod)
		}
		period := int32(n)
		input.FinalClusterSnapshotRetentionPeriod = &period
	}
	return nil
}

// deleteOptionsHint adds the delete annotations' names to a DeleteCluster
// error when the user set neither, so the condition message says what to add.
// The missing-snapshot error is not one of DeleteCluster's modeled faults, so
// those are passed through without the hint. An API error is rebuilt with the
// same code and fault and the hint appended to its message, so classification
// is unchanged; other errors are wrapped with %w.
func deleteOptionsHint(annotations map[string]string, err error) error {
	if err == nil {
		return nil
	}
	if awsErr, ok := ackerr.AWSError(err); ok {
		switch awsErr.ErrorCode() {
		case "ClusterNotFound", "ClusterSnapshotAlreadyExists", "ClusterSnapshotQuotaExceeded",
			"InvalidClusterState", "InvalidRetentionPeriodFault":
			return err
		}
	}
	if _, ok := annotations[AnnotationSkipFinalSnapshot]; ok {
		return err
	}
	if v, ok := annotations[AnnotationFinalSnapshotIdentifier]; ok && v != "" {
		return err
	}
	hint := fmt.Sprintf("; to delete the cluster, set annotation %s: \"true\" or %s: <snapshot-id>",
		AnnotationSkipFinalSnapshot, AnnotationFinalSnapshotIdentifier)
	// The generated updateConditions writes the inner API error's own
	// message to the condition, so a %w wrapper's text would only reach the
	// log. Rebuild the API error with the hint, keeping its code and fault.
	if awsErr, ok := ackerr.AWSError(err); ok {
		return &smithy.GenericAPIError{
			Code:    awsErr.ErrorCode(),
			Message: awsErr.ErrorMessage() + hint,
			Fault:   awsErr.ErrorFault(),
		}
	}
	return fmt.Errorf("%w%s", err, hint)
}

// iamRoleChanges returns the role ARNs to add and remove (sorted) to move
// latest to desired, ignoring order and duplicates.
func iamRoleChanges(desired, latest []*string) (add, remove []string) {
	d := map[string]struct{}{}
	for _, r := range desired {
		if r != nil {
			d[*r] = struct{}{}
		}
	}
	l := map[string]struct{}{}
	for _, r := range latest {
		if r != nil {
			l[*r] = struct{}{}
		}
	}
	for r := range d {
		if _, ok := l[r]; !ok {
			add = append(add, r)
		}
	}
	for r := range l {
		if _, ok := d[r]; !ok {
			remove = append(remove, r)
		}
	}
	sort.Strings(add)
	sort.Strings(remove)
	return add, remove
}

// defaultRoleChanged reports whether the user declared a default IAM role
// that differs from the observed one. A nil desired value is never a change:
// the API has no way to clear the default role.
func defaultRoleChanged(desired, latest *svcapitypes.Cluster) bool {
	return desired.Spec.DefaultIAMRoleARN != nil &&
		(latest.Spec.DefaultIAMRoleARN == nil || *desired.Spec.DefaultIAMRoleARN != *latest.Spec.DefaultIAMRoleARN)
}

// iamRolesInput builds the single ModifyClusterIamRoles request for the
// roles and default role difference, or nil when there is nothing to do.
func iamRolesInput(desired, latest *svcapitypes.Cluster) *svcsdk.ModifyClusterIamRolesInput {
	add, remove := iamRoleChanges(desired.Spec.IAMRoles, latest.Spec.IAMRoles)
	changeDefault := defaultRoleChanged(desired, latest)
	if len(add) == 0 && len(remove) == 0 && !changeDefault {
		return nil
	}
	input := &svcsdk.ModifyClusterIamRolesInput{
		ClusterIdentifier: desired.Spec.Identifier,
		AddIamRoles:       add,
		RemoveIamRoles:    remove,
	}
	if changeDefault {
		input.DefaultIamRoleArn = desired.Spec.DefaultIAMRoleARN
	}
	return input
}

// iamRolesNeedRequeue reports whether the ModifyClusterIamRoles response
// shows the cluster still applying the change. A response without a status is
// treated as not settled.
func iamRolesNeedRequeue(cluster *svcsdktypes.Cluster) bool {
	if cluster == nil || cluster.ClusterStatus == nil {
		return true
	}
	if *cluster.ClusterStatus != clusterStatusAvailable {
		return true
	}
	for _, role := range cluster.IamRoles {
		if role.ApplyStatus != nil && *role.ApplyStatus != iamRoleApplyInSync {
			return true
		}
	}
	return false
}

func requeueAfterModify(what string) error {
	return ackrequeue.NeededAfter(
		fmt.Errorf("%s modified, requeuing before applying further changes", what),
		requeueAfterSecondaryModify,
	)
}

// syncIAMRoles reconciles IamRoles (and a changed DefaultIamRoleArn) with one
// ModifyClusterIamRoles call. It is invoked from the generated custom_sync
// block, which passes no delta, so the difference is recomputed here.
func (rm *resourceManager) syncIAMRoles(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncIAMRoles")
	defer func() { exit(err) }()

	input := iamRolesInput(desired.ko, latest.ko)
	if input == nil {
		return nil
	}
	resp, err := rm.sdkapi.ModifyClusterIamRoles(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "ModifyClusterIamRoles", err)
	if err != nil {
		return err
	}
	if iamRolesNeedRequeue(resp.Cluster) {
		return requeueAfterModify("IAM roles")
	}
	return nil
}

// syncDefaultIAMRoleARN changes the default IAM role when the roles
// themselves are in sync; otherwise syncIAMRoles already sent it in the same
// call.
func (rm *resourceManager) syncDefaultIAMRoleARN(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncDefaultIAMRoleARN")
	defer func() { exit(err) }()

	if add, remove := iamRoleChanges(desired.ko.Spec.IAMRoles, latest.ko.Spec.IAMRoles); len(add) > 0 || len(remove) > 0 {
		return nil
	}
	input := iamRolesInput(desired.ko, latest.ko)
	if input == nil {
		return nil
	}
	resp, err := rm.sdkapi.ModifyClusterIamRoles(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "ModifyClusterIamRoles", err)
	if err != nil {
		return err
	}
	if iamRolesNeedRequeue(resp.Cluster) {
		return requeueAfterModify("default IAM role")
	}
	return nil
}

// snapshotScheduleInput builds the ModifyClusterSnapshotSchedule request, or
// nil when desired and latest agree.
func snapshotScheduleInput(desired, latest *svcapitypes.Cluster) *svcsdk.ModifyClusterSnapshotScheduleInput {
	d, l := desired.Spec.SnapshotScheduleIdentifier, latest.Spec.SnapshotScheduleIdentifier
	switch {
	case d != nil && (l == nil || *d != *l):
		return &svcsdk.ModifyClusterSnapshotScheduleInput{
			ClusterIdentifier:  desired.Spec.Identifier,
			ScheduleIdentifier: d,
		}
	case d == nil && l != nil:
		return &svcsdk.ModifyClusterSnapshotScheduleInput{
			ClusterIdentifier:    desired.Spec.Identifier,
			DisassociateSchedule: aws.Bool(true),
		}
	}
	return nil
}

// syncSnapshotScheduleIdentifier associates or disassociates the snapshot
// schedule. The response has no cluster status, so a real call always
// requeues.
func (rm *resourceManager) syncSnapshotScheduleIdentifier(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncSnapshotScheduleIdentifier")
	defer func() { exit(err) }()

	input := snapshotScheduleInput(desired.ko, latest.ko)
	if input == nil {
		return nil
	}
	_, err = rm.sdkapi.ModifyClusterSnapshotSchedule(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "ModifyClusterSnapshotSchedule", err)
	if err != nil {
		return err
	}
	return requeueAfterModify("snapshot schedule")
}

// syncTags reconciles the Tags field by computing the diff between desired
// and latest tags, then calling the Redshift CreateTags/DeleteTags APIs.
//
// This method is called by the generated sdkUpdate code when the
// custom_sync configuration detects a difference in Spec.Tags.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTags")
	defer func() { exit(err) }()

	// The ARN is set on Status.ACKResourceMetadata by the create and read hooks.
	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return fmt.Errorf("cannot sync tags: resource ARN is not available")
	}
	arn := string(*latest.ko.Status.ACKResourceMetadata.ARN)

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
