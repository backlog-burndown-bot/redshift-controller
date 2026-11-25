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
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/redshift/types"
	smithy "github.com/aws/smithy-go"

	svcapitypes "github.com/aws-controllers-k8s/redshift-controller/apis/v1alpha1"
)

func newCluster(mutate func(*svcapitypes.Cluster)) *svcapitypes.Cluster {
	ko := &svcapitypes.Cluster{}
	ko.Spec.Identifier = aws.String("my-cluster")
	if mutate != nil {
		mutate(ko)
	}
	return ko
}

func deltaWith(paths ...string) *ackcompare.Delta {
	d := ackcompare.NewDelta()
	for _, p := range paths {
		d.Add(p, "a", "b")
	}
	return d
}

// fakeTransport records each request's form body and replies with a canned
// response per Action.
type fakeTransport struct {
	responses map[string]string
	calls     []url.Values
}

func (f *fakeTransport) Do(req *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(req.Body)
	form, _ := url.ParseQuery(string(body))
	f.calls = append(f.calls, form)
	xml := f.responses[form.Get("Action")]
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"text/xml"}},
		Body:       io.NopCloser(strings.NewReader(xml)),
		Request:    req,
	}, nil
}

func (f *fakeTransport) actions() []string {
	var out []string
	for _, c := range f.calls {
		out = append(out, c.Get("Action"))
	}
	return out
}

func responseXML(action, result string) string {
	return `<` + action + `Response xmlns="http://redshift.amazonaws.com/doc/2012-12-01/">` +
		`<` + action + `Result>` + result + `</` + action + `Result>` +
		`<ResponseMetadata><RequestId>req</RequestId></ResponseMetadata></` + action + `Response>`
}

func newFakeManager(responses map[string]string) (*resourceManager, *fakeTransport) {
	ft := &fakeTransport{responses: responses}
	client := svcsdk.New(svcsdk.Options{
		Region:      "us-west-2",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  ft,
		Retryer:     aws.NopRetryer{},
	})
	return &resourceManager{
		sdkapi:       client,
		metrics:      ackmetrics.NewMetrics("redshift"),
		awsPartition: "aws",
		awsRegion:    "us-west-2",
		awsAccountID: "123456789012",
	}, ft
}

func isRequeue(err error) bool {
	var r *ackrequeue.RequeueNeededAfter
	return errors.As(err, &r)
}

func TestClusterARN(t *testing.T) {
	rm := &resourceManager{awsPartition: "aws-cn", awsRegion: "cn-north-1", awsAccountID: "123456789012"}
	got := rm.clusterARN("my-cluster")
	want := "arn:aws-cn:redshift:cn-north-1:123456789012:cluster:my-cluster"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestParseStatusBool(t *testing.T) {
	for in, want := range map[string][2]bool{
		"true": {true, true}, "Enabled": {true, true}, "false": {false, true},
		"DISABLED": {false, true}, "": {false, false}, "maybe": {false, false},
	} {
		v, ok := parseStatusBool(in)
		if v != want[0] || ok != want[1] {
			t.Errorf("%q: got (%v,%v) want (%v,%v)", in, v, ok, want[0], want[1])
		}
	}
}

func TestSetObservedSpecFields(t *testing.T) {
	observed := &svcsdktypes.Cluster{
		ClusterParameterGroups:               []svcsdktypes.ClusterParameterGroupStatus{{ParameterGroupName: aws.String("pg")}},
		ElasticIpStatus:                      &svcsdktypes.ElasticIpStatus{ElasticIp: aws.String("1.2.3.4")},
		HsmStatus:                            &svcsdktypes.HsmStatus{HsmClientCertificateIdentifier: aws.String("cert"), HsmConfigurationIdentifier: aws.String("cfg")},
		Endpoint:                             &svcsdktypes.Endpoint{Port: aws.Int32(5439)},
		NumberOfNodes:                        aws.Int32(1),
		MasterPasswordSecretArn:              aws.String("arn:secret"),
		MultiAZ:                              aws.String("Enabled"),
		ExtraComputeForAutomaticOptimization: aws.String("false"),
		AvailabilityZoneRelocationStatus:     aws.String("enabled"),
	}

	t.Run("declared fields are recovered", func(t *testing.T) {
		ko := newCluster(func(c *svcapitypes.Cluster) {
			c.Spec.ClusterParameterGroupName = aws.String("old")
			c.Spec.ElasticIP = aws.String("old")
			c.Spec.HsmClientCertificateIdentifier = aws.String("old")
			c.Spec.HsmConfigurationIdentifier = aws.String("old")
			c.Spec.Port = aws.Int64(1)
			c.Spec.Type = aws.String("multi-node")
			c.Spec.ManageMasterPassword = aws.Bool(false)
			c.Spec.MultiAZ = aws.Bool(false)
			c.Spec.ExtraComputeForAutomaticOptimization = aws.Bool(true)
			c.Spec.AvailabilityZoneRelocation = aws.Bool(false)
		})
		setObservedSpecFields(ko, observed)
		s := ko.Spec
		if *s.ClusterParameterGroupName != "pg" || *s.ElasticIP != "1.2.3.4" ||
			*s.HsmClientCertificateIdentifier != "cert" || *s.HsmConfigurationIdentifier != "cfg" ||
			*s.Port != 5439 || *s.Type != "single-node" || !*s.ManageMasterPassword ||
			!*s.MultiAZ || *s.ExtraComputeForAutomaticOptimization || !*s.AvailabilityZoneRelocation {
			t.Fatalf("unexpected spec after recovery: %+v", s)
		}
	})

	t.Run("undeclared fields stay nil", func(t *testing.T) {
		ko := newCluster(nil)
		setObservedSpecFields(ko, observed)
		want := newCluster(nil)
		if !reflect.DeepEqual(ko.Spec, want.Spec) {
			t.Fatalf("spec changed for undeclared fields: %+v", ko.Spec)
		}
	})

	t.Run("nil and unknown observed values keep the declared value", func(t *testing.T) {
		ko := newCluster(func(c *svcapitypes.Cluster) {
			c.Spec.ClusterParameterGroupName = aws.String("keep")
			c.Spec.ElasticIP = aws.String("keep")
			c.Spec.HsmClientCertificateIdentifier = aws.String("keep")
			c.Spec.Port = aws.Int64(7)
			c.Spec.Type = aws.String("multi-node")
			c.Spec.MultiAZ = aws.Bool(true)
			c.Spec.ExtraComputeForAutomaticOptimization = aws.Bool(true)
		})
		setObservedSpecFields(ko, &svcsdktypes.Cluster{
			MultiAZ:                              aws.String("weird"),
			ExtraComputeForAutomaticOptimization: aws.String(""),
		})
		s := ko.Spec
		if *s.ClusterParameterGroupName != "keep" || *s.ElasticIP != "keep" ||
			*s.HsmClientCertificateIdentifier != "keep" || *s.Port != 7 ||
			*s.Type != "multi-node" || !*s.MultiAZ || !*s.ExtraComputeForAutomaticOptimization {
			t.Fatalf("declared values were overwritten: %+v", s)
		}
	})

	t.Run("multi-node and manageMasterPassword false", func(t *testing.T) {
		ko := newCluster(func(c *svcapitypes.Cluster) {
			c.Spec.Type = aws.String("single-node")
			c.Spec.ManageMasterPassword = aws.Bool(true)
		})
		setObservedSpecFields(ko, &svcsdktypes.Cluster{NumberOfNodes: aws.Int32(3)})
		if *ko.Spec.Type != "multi-node" || *ko.Spec.ManageMasterPassword {
			t.Fatalf("got %+v", ko.Spec)
		}
	})
}

func TestObservedCluster(t *testing.T) {
	clusters := []svcsdktypes.Cluster{
		{ClusterIdentifier: aws.String("a")},
		{ClusterIdentifier: aws.String("b")},
	}
	if got := observedCluster(clusters, aws.String("b")); *got.ClusterIdentifier != "b" {
		t.Fatalf("got %v", got.ClusterIdentifier)
	}
	if got := observedCluster(clusters, aws.String("zz")); got != nil {
		t.Fatalf("no match: got %v, want nil", got.ClusterIdentifier)
	}
	if observedCluster(nil, nil) != nil {
		t.Fatal("expected nil for no clusters")
	}
}

func TestKeepEquivalentKMSKeyIDs(t *testing.T) {
	const id = "1234abcd-12ab-34cd-56ef-1234567890ab"
	const arn = "arn:aws:kms:us-west-2:111122223333:key/" + id
	for _, tc := range []struct {
		name            string
		latest, desired *string
		want            *string
	}{
		{"AWS returns ARN, user declared ID", aws.String(arn), aws.String(id), aws.String(id)},
		{"AWS returns ID, user declared ARN", aws.String(id), aws.String(arn), aws.String(arn)},
		{"different key stays different", aws.String(arn), aws.String("other"), aws.String(arn)},
		{"two ARNs of different keys stay different", aws.String(arn),
			aws.String("arn:aws:kms:us-west-2:111122223333:key/other"), aws.String(arn)},
		{"undeclared keeps observed", aws.String(arn), nil, aws.String(arn)},
		{"unset in AWS stays unset", nil, aws.String(id), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			latest := svcapitypes.ClusterSpec{KMSKeyID: tc.latest, MasterPasswordSecretKMSKeyID: tc.latest}
			desired := svcapitypes.ClusterSpec{KMSKeyID: tc.desired, MasterPasswordSecretKMSKeyID: tc.desired}
			keepEquivalentKMSKeyIDs(&latest, &desired)
			for field, got := range map[string]*string{
				"KMSKeyID": latest.KMSKeyID, "MasterPasswordSecretKMSKeyID": latest.MasterPasswordSecretKMSKeyID,
			} {
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("%s = %v, want %v", field, aws.ToString(got), aws.ToString(tc.want))
				}
			}
		})
	}
}

func TestDeleteOptionsHint(t *testing.T) {
	awsErr := errors.New("InvalidParameterCombination: FinalClusterSnapshotIdentifier is required")
	if deleteOptionsHint(nil, nil) != nil {
		t.Fatal("nil error must stay nil")
	}
	got := deleteOptionsHint(nil, awsErr)
	if !errors.Is(got, awsErr) {
		t.Errorf("hint must wrap the AWS error, got %v", got)
	}
	for _, want := range []string{AnnotationSkipFinalSnapshot, AnnotationFinalSnapshotIdentifier} {
		if !strings.Contains(got.Error(), want) {
			t.Errorf("hint %q does not name %s", got, want)
		}
	}
	for _, a := range []map[string]string{
		{AnnotationSkipFinalSnapshot: "false"},
		{AnnotationFinalSnapshotIdentifier: "snap"},
	} {
		if got := deleteOptionsHint(a, awsErr); got != awsErr {
			t.Errorf("annotations %v: got %v, want the AWS error unchanged", a, got)
		}
	}
	// Modeled DeleteCluster faults are unrelated to the missing snapshot.
	modeled := &smithy.GenericAPIError{Code: "InvalidClusterState", Message: "cluster is modifying"}
	if got := deleteOptionsHint(nil, modeled); got != error(modeled) {
		t.Errorf("modeled fault: got %v, want it unchanged", got)
	}
}

func TestApplyResizeCoupling(t *testing.T) {
	tests := []struct {
		name      string
		desired   func(*svcapitypes.Cluster)
		latest    func(*svcapitypes.Cluster)
		delta     []string
		wantNode  *string
		wantCount *int32
		wantType  *string
		wantFired bool
	}{
		{
			name: "late-initialized NumberOfNodes nil in desired does not fire",
			latest: func(c *svcapitypes.Cluster) {
				c.Spec.NodeType = aws.String("ra3.large")
				c.Spec.NumberOfNodes = aws.Int64(1)
			},
			desired: func(c *svcapitypes.Cluster) { c.Spec.NodeType = aws.String("ra3.large") },
			delta:   []string{"Spec.NumberOfNodes", "Spec.AvailabilityZone"},
		},
		{
			name: "declared NumberOfNodes takes NodeType from latest",
			latest: func(c *svcapitypes.Cluster) {
				c.Spec.NodeType = aws.String("ra3.large")
				c.Spec.NumberOfNodes = aws.Int64(2)
			},
			desired:   func(c *svcapitypes.Cluster) { c.Spec.NumberOfNodes = aws.Int64(4) },
			delta:     []string{"Spec.NumberOfNodes"},
			wantNode:  aws.String("ra3.large"),
			wantCount: aws.Int32(4),
			wantFired: true,
		},
		{
			name: "declared NodeType takes NumberOfNodes from latest",
			latest: func(c *svcapitypes.Cluster) {
				c.Spec.NodeType = aws.String("ra3.large")
				c.Spec.NumberOfNodes = aws.Int64(2)
			},
			desired:   func(c *svcapitypes.Cluster) { c.Spec.NodeType = aws.String("ra3.xlplus") },
			delta:     []string{"Spec.NodeType"},
			wantNode:  aws.String("ra3.xlplus"),
			wantCount: aws.Int32(2),
			wantFired: true,
		},
		{
			name: "declared NodeType with late-initialized non-nil NumberOfNodes carries both",
			latest: func(c *svcapitypes.Cluster) {
				c.Spec.NodeType = aws.String("ra3.large")
				c.Spec.NumberOfNodes = aws.Int64(1)
			},
			desired: func(c *svcapitypes.Cluster) {
				c.Spec.NodeType = aws.String("ra3.xlplus")
				c.Spec.NumberOfNodes = aws.Int64(1)
			},
			delta:     []string{"Spec.NodeType"},
			wantNode:  aws.String("ra3.xlplus"),
			wantCount: aws.Int32(1),
			wantFired: true,
		},
		{
			name: "declared Type couples the node members and sends Type",
			latest: func(c *svcapitypes.Cluster) {
				c.Spec.NodeType = aws.String("ra3.large")
				c.Spec.NumberOfNodes = aws.Int64(1)
			},
			desired:   func(c *svcapitypes.Cluster) { c.Spec.Type = aws.String("multi-node") },
			delta:     []string{"Spec.Type"},
			wantNode:  aws.String("ra3.large"),
			wantCount: aws.Int32(1),
			wantType:  aws.String("multi-node"),
			wantFired: true,
		},
		{
			name:    "unrelated delta does not fire",
			latest:  func(c *svcapitypes.Cluster) { c.Spec.NodeType = aws.String("ra3.large") },
			desired: func(c *svcapitypes.Cluster) { c.Spec.NodeType = aws.String("ra3.large") },
			delta:   []string{"Spec.PubliclyAccessible"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := &svcsdk.ModifyClusterInput{ClusterIdentifier: aws.String("my-cluster")}
			err := applyResizeCoupling(input, newCluster(tc.desired), newCluster(tc.latest), deltaWith(tc.delta...))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(input.NodeType, tc.wantNode) ||
				!reflect.DeepEqual(input.NumberOfNodes, tc.wantCount) ||
				!reflect.DeepEqual(input.ClusterType, tc.wantType) {
				t.Fatalf("got node=%v count=%v type=%v", aws.ToString(input.NodeType), input.NumberOfNodes, input.ClusterType)
			}
			if !tc.wantFired && !modifyClusterInputIsEmpty(input) {
				t.Fatal("request should stay identifier-only")
			}
			if tc.wantFired && modifyClusterInputIsEmpty(input) {
				t.Fatal("request should not be skipped")
			}
		})
	}
}

func TestModifyClusterInputIsEmpty(t *testing.T) {
	// Distinct pointers with equal values must compare equal.
	if !modifyClusterInputIsEmpty(&svcsdk.ModifyClusterInput{ClusterIdentifier: aws.String("x")}) {
		t.Fatal("identifier-only input should be empty")
	}
	if modifyClusterInputIsEmpty(&svcsdk.ModifyClusterInput{
		ClusterIdentifier:                aws.String("x"),
		AutomatedSnapshotRetentionPeriod: aws.Int32(2),
	}) {
		t.Fatal("input with an extra member should not be empty")
	}
	if modifyClusterInputIsEmpty(&svcsdk.ModifyClusterInput{
		ClusterIdentifier:   aws.String("x"),
		VpcSecurityGroupIds: []string{"sg-1"},
	}) {
		t.Fatal("input with a list member should not be empty")
	}
}

func TestSetDeleteOptions(t *testing.T) {
	t.Run("absent sends nothing", func(t *testing.T) {
		in := &svcsdk.DeleteClusterInput{}
		if err := setDeleteOptions(nil, in); err != nil {
			t.Fatal(err)
		}
		if in.SkipFinalClusterSnapshot != nil || in.FinalClusterSnapshotIdentifier != nil || in.FinalClusterSnapshotRetentionPeriod != nil {
			t.Fatalf("unexpected options: %+v", in)
		}
	})
	t.Run("skip true", func(t *testing.T) {
		in := &svcsdk.DeleteClusterInput{}
		if err := setDeleteOptions(map[string]string{AnnotationSkipFinalSnapshot: "true"}, in); err != nil {
			t.Fatal(err)
		}
		if in.SkipFinalClusterSnapshot == nil || !*in.SkipFinalClusterSnapshot {
			t.Fatal("expected skip=true")
		}
	})
	t.Run("skip false with identifier and retention", func(t *testing.T) {
		in := &svcsdk.DeleteClusterInput{}
		err := setDeleteOptions(map[string]string{
			AnnotationSkipFinalSnapshot:            "false",
			AnnotationFinalSnapshotIdentifier:      "final",
			AnnotationFinalSnapshotRetentionPeriod: "-1",
		}, in)
		if err != nil {
			t.Fatal(err)
		}
		if *in.SkipFinalClusterSnapshot || *in.FinalClusterSnapshotIdentifier != "final" || *in.FinalClusterSnapshotRetentionPeriod != -1 {
			t.Fatalf("got %+v", in)
		}
	})
	t.Run("invalid values are retryable errors naming the annotation", func(t *testing.T) {
		for _, a := range []map[string]string{
			{AnnotationSkipFinalSnapshot: "yes"},
			{AnnotationFinalSnapshotRetentionPeriod: "ten"},
		} {
			err := setDeleteOptions(a, &svcsdk.DeleteClusterInput{})
			if err == nil {
				t.Fatalf("expected error for %v", a)
			}
			var key string
			for k := range a {
				key = k
			}
			if !strings.Contains(err.Error(), key) {
				t.Fatalf("error %q does not name %s", err, key)
			}
			if isRequeue(err) {
				t.Fatal("must be a plain error")
			}
		}
	})
}

func TestIAMRoleChanges(t *testing.T) {
	add, remove := iamRoleChanges(
		[]*string{aws.String("b"), aws.String("a"), aws.String("a"), nil},
		[]*string{aws.String("c"), aws.String("b")},
	)
	if !reflect.DeepEqual(add, []string{"a"}) || !reflect.DeepEqual(remove, []string{"c"}) {
		t.Fatalf("add=%v remove=%v", add, remove)
	}
	add, remove = iamRoleChanges([]*string{aws.String("x"), aws.String("y")}, []*string{aws.String("y"), aws.String("x")})
	if len(add) != 0 || len(remove) != 0 {
		t.Fatalf("order must not matter: add=%v remove=%v", add, remove)
	}
}

func TestIAMRolesInput(t *testing.T) {
	roles := func(r ...string) []*string { return aws.StringSlice(r) }
	t.Run("nothing to do", func(t *testing.T) {
		d := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles("a"); c.Spec.DefaultIAMRoleARN = aws.String("a") })
		l := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles("a"); c.Spec.DefaultIAMRoleARN = aws.String("a") })
		if iamRolesInput(d, l) != nil {
			t.Fatal("expected no request")
		}
	})
	t.Run("nil desired default is not a change", func(t *testing.T) {
		d := newCluster(nil)
		l := newCluster(func(c *svcapitypes.Cluster) { c.Spec.DefaultIAMRoleARN = aws.String("a") })
		if iamRolesInput(d, l) != nil {
			t.Fatal("expected no request")
		}
	})
	t.Run("roles and default combine into one request", func(t *testing.T) {
		d := newCluster(func(c *svcapitypes.Cluster) {
			c.Spec.IAMRoles = roles("a", "b")
			c.Spec.DefaultIAMRoleARN = aws.String("b")
		})
		l := newCluster(func(c *svcapitypes.Cluster) {
			c.Spec.IAMRoles = roles("a", "c")
			c.Spec.DefaultIAMRoleARN = aws.String("a")
		})
		in := iamRolesInput(d, l)
		if in == nil || !reflect.DeepEqual(in.AddIamRoles, []string{"b"}) || !reflect.DeepEqual(in.RemoveIamRoles, []string{"c"}) || *in.DefaultIamRoleArn != "b" {
			t.Fatalf("got %+v", in)
		}
	})
	t.Run("roles only leaves default unset", func(t *testing.T) {
		d := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles("a") })
		l := newCluster(nil)
		in := iamRolesInput(d, l)
		if in == nil || in.DefaultIamRoleArn != nil || !reflect.DeepEqual(in.AddIamRoles, []string{"a"}) {
			t.Fatalf("got %+v", in)
		}
	})
	t.Run("default only", func(t *testing.T) {
		d := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles("a"); c.Spec.DefaultIAMRoleARN = aws.String("a") })
		l := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles("a") })
		in := iamRolesInput(d, l)
		if in == nil || len(in.AddIamRoles) != 0 || len(in.RemoveIamRoles) != 0 || *in.DefaultIamRoleArn != "a" {
			t.Fatalf("got %+v", in)
		}
	})
}

func TestIAMRolesNeedRequeue(t *testing.T) {
	st := func(s string) *string { return &s }
	tests := []struct {
		name    string
		cluster *svcsdktypes.Cluster
		want    bool
	}{
		{"nil cluster", nil, true},
		{"no status", &svcsdktypes.Cluster{}, true},
		{"modifying", &svcsdktypes.Cluster{ClusterStatus: st("modifying")}, true},
		{"available but adding", &svcsdktypes.Cluster{ClusterStatus: st("available"), IamRoles: []svcsdktypes.ClusterIamRole{{ApplyStatus: st("adding")}}}, true},
		{"available and in-sync", &svcsdktypes.Cluster{ClusterStatus: st("available"), IamRoles: []svcsdktypes.ClusterIamRole{{ApplyStatus: st("in-sync")}}}, false},
		{"available, no roles", &svcsdktypes.Cluster{ClusterStatus: st("available")}, false},
	}
	for _, tc := range tests {
		if got := iamRolesNeedRequeue(tc.cluster); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func resourceOf(ko *svcapitypes.Cluster) *resource { return &resource{ko: ko} }

const iamRolesAvailable = `<Cluster><ClusterIdentifier>my-cluster</ClusterIdentifier><ClusterStatus>available</ClusterStatus>` +
	`<IamRoles><ClusterIamRole><IamRoleArn>a</IamRoleArn><ApplyStatus>in-sync</ApplyStatus></ClusterIamRole></IamRoles></Cluster>`
const iamRolesModifying = `<Cluster><ClusterIdentifier>my-cluster</ClusterIdentifier><ClusterStatus>modifying</ClusterStatus></Cluster>`

func TestSyncIAMRoles(t *testing.T) {
	roles := func(r ...string) []*string { return aws.StringSlice(r) }
	desired := newCluster(func(c *svcapitypes.Cluster) {
		c.Spec.IAMRoles = roles("a", "b")
		c.Spec.DefaultIAMRoleARN = aws.String("b")
	})
	latest := newCluster(func(c *svcapitypes.Cluster) {
		c.Spec.IAMRoles = roles("a", "c")
		c.Spec.DefaultIAMRoleARN = aws.String("a")
	})

	t.Run("one call carries add, remove and default; requeues while modifying", func(t *testing.T) {
		rm, ft := newFakeManager(map[string]string{"ModifyClusterIamRoles": responseXML("ModifyClusterIamRoles", iamRolesModifying)})
		err := rm.syncIAMRoles(context.Background(), resourceOf(desired), resourceOf(latest))
		if !isRequeue(err) {
			t.Fatalf("expected requeue, got %v", err)
		}
		if got := ft.actions(); !reflect.DeepEqual(got, []string{"ModifyClusterIamRoles"}) {
			t.Fatalf("calls: %v", got)
		}
		c := ft.calls[0]
		if c.Get("AddIamRoles.IamRoleArn.1") != "b" || c.Get("RemoveIamRoles.IamRoleArn.1") != "c" || c.Get("DefaultIamRoleArn") != "b" {
			t.Fatalf("request: %v", c)
		}
	})
	t.Run("default handler defers to the roles call", func(t *testing.T) {
		rm, ft := newFakeManager(nil)
		if err := rm.syncDefaultIAMRoleARN(context.Background(), resourceOf(desired), resourceOf(latest)); err != nil {
			t.Fatal(err)
		}
		if len(ft.calls) != 0 {
			t.Fatalf("unexpected calls: %v", ft.actions())
		}
	})
	t.Run("no requeue when the response is settled", func(t *testing.T) {
		rm, ft := newFakeManager(map[string]string{"ModifyClusterIamRoles": responseXML("ModifyClusterIamRoles", iamRolesAvailable)})
		if err := rm.syncIAMRoles(context.Background(), resourceOf(desired), resourceOf(latest)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ft.calls) != 1 {
			t.Fatalf("calls: %v", ft.actions())
		}
	})
	t.Run("nothing to do makes no call and no requeue", func(t *testing.T) {
		rm, ft := newFakeManager(nil)
		same := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles("a") })
		if err := rm.syncIAMRoles(context.Background(), resourceOf(same), resourceOf(same.DeepCopy())); err != nil {
			t.Fatal(err)
		}
		if len(ft.calls) != 0 {
			t.Fatalf("unexpected calls: %v", ft.actions())
		}
	})
}

func TestSyncDefaultIAMRoleARN(t *testing.T) {
	roles := aws.StringSlice([]string{"a"})
	desired := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles; c.Spec.DefaultIAMRoleARN = aws.String("a") })
	latest := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles })

	t.Run("sends the default alone", func(t *testing.T) {
		rm, ft := newFakeManager(map[string]string{"ModifyClusterIamRoles": responseXML("ModifyClusterIamRoles", iamRolesAvailable)})
		if err := rm.syncDefaultIAMRoleARN(context.Background(), resourceOf(desired), resourceOf(latest)); err != nil {
			t.Fatal(err)
		}
		if len(ft.calls) != 1 || ft.calls[0].Get("DefaultIamRoleArn") != "a" || ft.calls[0].Get("AddIamRoles.IamRoleArn.1") != "" {
			t.Fatalf("calls: %v", ft.calls)
		}
	})
	t.Run("requeues when cluster is modifying", func(t *testing.T) {
		rm, _ := newFakeManager(map[string]string{"ModifyClusterIamRoles": responseXML("ModifyClusterIamRoles", iamRolesModifying)})
		if err := rm.syncDefaultIAMRoleARN(context.Background(), resourceOf(desired), resourceOf(latest)); !isRequeue(err) {
			t.Fatalf("expected requeue, got %v", err)
		}
	})
	t.Run("nil desired makes no call", func(t *testing.T) {
		rm, ft := newFakeManager(nil)
		d := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles })
		l := newCluster(func(c *svcapitypes.Cluster) { c.Spec.IAMRoles = roles; c.Spec.DefaultIAMRoleARN = aws.String("a") })
		if err := rm.syncDefaultIAMRoleARN(context.Background(), resourceOf(d), resourceOf(l)); err != nil {
			t.Fatal(err)
		}
		if len(ft.calls) != 0 {
			t.Fatalf("unexpected calls: %v", ft.actions())
		}
	})
}

func TestSyncSnapshotScheduleIdentifier(t *testing.T) {
	resp := map[string]string{"ModifyClusterSnapshotSchedule": responseXML("ModifyClusterSnapshotSchedule", "")}

	t.Run("associate requeues unconditionally", func(t *testing.T) {
		rm, ft := newFakeManager(resp)
		d := newCluster(func(c *svcapitypes.Cluster) { c.Spec.SnapshotScheduleIdentifier = aws.String("sched") })
		err := rm.syncSnapshotScheduleIdentifier(context.Background(), resourceOf(d), resourceOf(newCluster(nil)))
		if !isRequeue(err) {
			t.Fatalf("expected requeue, got %v", err)
		}
		if len(ft.calls) != 1 || ft.calls[0].Get("ScheduleIdentifier") != "sched" || ft.calls[0].Get("DisassociateSchedule") != "" {
			t.Fatalf("calls: %v", ft.calls)
		}
	})
	t.Run("disassociate when desired is nil", func(t *testing.T) {
		rm, ft := newFakeManager(resp)
		l := newCluster(func(c *svcapitypes.Cluster) { c.Spec.SnapshotScheduleIdentifier = aws.String("sched") })
		err := rm.syncSnapshotScheduleIdentifier(context.Background(), resourceOf(newCluster(nil)), resourceOf(l))
		if !isRequeue(err) {
			t.Fatalf("expected requeue, got %v", err)
		}
		if len(ft.calls) != 1 || ft.calls[0].Get("DisassociateSchedule") != "true" || ft.calls[0].Get("ScheduleIdentifier") != "" {
			t.Fatalf("calls: %v", ft.calls)
		}
	})
	t.Run("no difference means no call and no requeue", func(t *testing.T) {
		rm, ft := newFakeManager(resp)
		same := newCluster(func(c *svcapitypes.Cluster) { c.Spec.SnapshotScheduleIdentifier = aws.String("sched") })
		if err := rm.syncSnapshotScheduleIdentifier(context.Background(), resourceOf(same), resourceOf(same.DeepCopy())); err != nil {
			t.Fatal(err)
		}
		if err := rm.syncSnapshotScheduleIdentifier(context.Background(), resourceOf(newCluster(nil)), resourceOf(newCluster(nil))); err != nil {
			t.Fatal(err)
		}
		if len(ft.calls) != 0 {
			t.Fatalf("unexpected calls: %v", ft.actions())
		}
	})
}

func TestSyncTags(t *testing.T) {
	tag := func(k, v string) *svcapitypes.Tag { return &svcapitypes.Tag{Key: aws.String(k), Value: aws.String(v)} }
	arn := ackv1alpha1.AWSResourceName("arn:aws:redshift:us-west-2:123456789012:cluster:my-cluster")
	latestWith := func(tags ...*svcapitypes.Tag) *resource {
		ko := newCluster(func(c *svcapitypes.Cluster) { c.Spec.Tags = tags })
		ko.Status.ACKResourceMetadata = &ackv1alpha1.ResourceMetadata{ARN: &arn}
		return resourceOf(ko)
	}
	resp := map[string]string{
		"CreateTags": `<CreateTagsResponse xmlns="http://redshift.amazonaws.com/doc/2012-12-01/"><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></CreateTagsResponse>`,
		"DeleteTags": `<DeleteTagsResponse xmlns="http://redshift.amazonaws.com/doc/2012-12-01/"><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></DeleteTagsResponse>`,
	}

	t.Run("adds, updates and removes", func(t *testing.T) {
		rm, ft := newFakeManager(resp)
		desired := newCluster(func(c *svcapitypes.Cluster) {
			c.Spec.Tags = []*svcapitypes.Tag{tag("keep", "1"), tag("chg", "new"), tag("add", "x")}
		})
		err := rm.syncTags(context.Background(), resourceOf(desired), latestWith(tag("keep", "1"), tag("chg", "old"), tag("gone", "y")))
		if err != nil {
			t.Fatal(err)
		}
		if got := ft.actions(); !reflect.DeepEqual(got, []string{"DeleteTags", "CreateTags"}) {
			t.Fatalf("calls: %v", got)
		}
		if ft.calls[0].Get("ResourceName") != string(arn) || ft.calls[0].Get("TagKeys.TagKey.1") != "gone" {
			t.Fatalf("delete request: %v", ft.calls[0])
		}
		created := map[string]string{}
		for i := 1; i <= 2; i++ {
			created[ft.calls[1].Get("Tags.Tag."+string(rune('0'+i))+".Key")] = ft.calls[1].Get("Tags.Tag." + string(rune('0'+i)) + ".Value")
		}
		if !reflect.DeepEqual(created, map[string]string{"chg": "new", "add": "x"}) {
			t.Fatalf("create request: %v", ft.calls[1])
		}
	})
	t.Run("identical tags make no call", func(t *testing.T) {
		rm, ft := newFakeManager(resp)
		desired := newCluster(func(c *svcapitypes.Cluster) { c.Spec.Tags = []*svcapitypes.Tag{tag("a", "1")} })
		if err := rm.syncTags(context.Background(), resourceOf(desired), latestWith(tag("a", "1"))); err != nil {
			t.Fatal(err)
		}
		if len(ft.calls) != 0 {
			t.Fatalf("unexpected calls: %v", ft.actions())
		}
	})
	t.Run("missing ARN is an error", func(t *testing.T) {
		rm, _ := newFakeManager(resp)
		if err := rm.syncTags(context.Background(), resourceOf(newCluster(nil)), resourceOf(newCluster(nil))); err == nil {
			t.Fatal("expected error")
		}
	})
}
