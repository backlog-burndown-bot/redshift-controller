# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#         http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the Redshift ClusterParameterGroup resource."""

import copy
import json
import logging
import time

import pytest
from botocore.exceptions import ClientError

from acktest import tags as tagutil
from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_redshift_resource
from e2e.replacement_values import REPLACEMENT_VALUES

RESOURCE_PLURAL = "clusterparametergroups"

MODIFY_WAIT_AFTER_SECONDS = 10
DELETE_WAIT_AFTER_SECONDS = 10
SYNC_WAIT_PERIODS = 12
SYNC_PERIOD_LENGTH = 10

STATEMENT_TIMEOUT = "statement_timeout"
USER_ACTIVITY_LOGGING = "enable_user_activity_logging"
WLM_JSON_CONFIGURATION = "wlm_json_configuration"
# One manual WLM queue with a concurrency level of five.
WLM_VALUE = '[{"query_concurrency":5}]'
STABILITY_WAIT_SECONDS = 30


def get_family(redshift_client):
    """Pick a parameter group family from the account's default.* groups
    rather than hardcoding one, preferring redshift-2.0."""
    families = set()
    marker = None
    while True:
        kwargs = {"Marker": marker} if marker else {}
        resp = redshift_client.describe_cluster_parameter_groups(**kwargs)
        for g in resp["ParameterGroups"]:
            if g["ParameterGroupName"].startswith("default."):
                families.add(g["ParameterGroupFamily"])
        marker = resp.get("Marker")
        if not marker:
            break
    assert families, "no default.* cluster parameter groups found"
    return "redshift-2.0" if "redshift-2.0" in families else sorted(families)[-1]


def get_default_parameters(redshift_client, family):
    defaults = {}
    marker = None
    while True:
        kwargs = {"ParameterGroupFamily": family}
        if marker:
            kwargs["Marker"] = marker
        resp = redshift_client.describe_default_cluster_parameters(**kwargs)
        page = resp["DefaultClusterParameters"]
        for p in page.get("Parameters", []):
            defaults[p["ParameterName"]] = p
        marker = page.get("Marker")
        if not marker:
            break
    return defaults


def get_aws_parameters(redshift_client, name):
    params = {}
    marker = None
    while True:
        kwargs = {"ParameterGroupName": name}
        if marker:
            kwargs["Marker"] = marker
        resp = redshift_client.describe_cluster_parameters(**kwargs)
        for p in resp.get("Parameters", []):
            params[p["ParameterName"]] = p
        marker = resp.get("Marker")
        if not marker:
            break
    return params


def get_aws_group(redshift_client, name):
    resp = redshift_client.describe_cluster_parameter_groups(
        ParameterGroupName=name,
    )
    assert len(resp["ParameterGroups"]) == 1
    return resp["ParameterGroups"][0]


def pick_different(default, *candidates):
    return next(c for c in candidates if c != default)


def status_by_name(cr):
    statuses = cr.get("status", {}).get("parameterOverrideStatuses") or []
    return {s["parameterName"]: s for s in statuses}


def assert_synced(ref):
    assert k8s.wait_on_condition(
        ref, "ACK.ResourceSynced", "True",
        wait_periods=SYNC_WAIT_PERIODS, period_length=SYNC_PERIOD_LENGTH,
    )


def patch_and_assert_synced(ref, patch):
    # Wait for a Synced=True written by a reconcile that ran after the patch,
    # not one left over from the previous generation.
    before = condition.get_synced_last_transition_time(ref)
    k8s.patch_custom_resource(ref, patch)
    time.sleep(MODIFY_WAIT_AFTER_SECONDS)
    assert k8s.wait_on_condition_after(
        ref, "ACK.ResourceSynced", "True",
        last_transition_after=before,
        wait_periods=SYNC_WAIT_PERIODS, period_length=SYNC_PERIOD_LENGTH,
    )


@service_marker
@pytest.mark.canary
class TestClusterParameterGroup:
    def test_create_update_delete(self, redshift_client):
        family = get_family(redshift_client)
        defaults = get_default_parameters(redshift_client, family)
        for p in (STATEMENT_TIMEOUT, USER_ACTIVITY_LOGGING, WLM_JSON_CONFIGURATION):
            assert p in defaults, f"{p} missing from {family} defaults"
            assert defaults[p]["IsModifiable"], f"{p} is not modifiable"

        timeout_default = defaults[STATEMENT_TIMEOUT].get("ParameterValue")
        logging_default = defaults[USER_ACTIVITY_LOGGING].get("ParameterValue")
        timeout_1 = pick_different(timeout_default, "30000", "45000")
        timeout_2 = pick_different(timeout_default, "60000", "75000")
        logging_value = "true" if logging_default == "false" else "false"
        wlm_value = pick_different(
            defaults[WLM_JSON_CONFIGURATION].get("ParameterValue"), WLM_VALUE,
        )

        resource_name = random_suffix_name("ack-test-cpg", 24)
        description = "ACK test cluster parameter group"

        replacements = REPLACEMENT_VALUES.copy()
        replacements["CLUSTER_PARAMETER_GROUP_NAME"] = resource_name
        replacements["FAMILY"] = family
        replacements["DESCRIPTION"] = description
        replacements["PARAM_STATEMENT_TIMEOUT"] = timeout_1
        replacements["PARAM_ENABLE_USER_ACTIVITY_LOGGING"] = logging_value

        resource_data = load_redshift_resource(
            "cluster_parameter_group",
            additional_replacements=replacements,
        )
        logging.info(f"Creating ClusterParameterGroup {resource_name}")

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            resource_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)
        assert cr is not None
        assert k8s.get_resource_exists(ref)

        try:
            # --- Create: description, parameters and tags reach AWS ---
            assert_synced(ref)
            group = get_aws_group(redshift_client, resource_name)
            assert group["ParameterGroupFamily"] == family
            assert group["Description"] == description
            # assert_equal_without_ack_tags drops services.k8s.aws/* tags. The
            # other helm resourceTags defaults (app.kubernetes.io/managed-by,
            # kro.run/kro-version) are filled from CR labels and skipped when
            # empty; this CR carries no such labels.
            tagutil.assert_equal_without_ack_tags(
                {"environment": "test", "team": "data"},
                {t["Key"]: t["Value"] for t in group.get("Tags", [])},
            )
            params = get_aws_parameters(redshift_client, resource_name)
            assert params[STATEMENT_TIMEOUT]["ParameterValue"] == timeout_1
            assert params[STATEMENT_TIMEOUT]["Source"] == "user"
            assert params[USER_ACTIVITY_LOGGING]["ParameterValue"] == logging_value
            assert params[USER_ACTIVITY_LOGGING]["Source"] == "user"
            statuses = status_by_name(k8s.get_resource(ref))
            assert set(statuses) == {STATEMENT_TIMEOUT, USER_ACTIVITY_LOGGING}
            assert statuses[STATEMENT_TIMEOUT]["parameterValue"] == timeout_1
            assert statuses[USER_ACTIVITY_LOGGING]["parameterValue"] == logging_value
            for s in statuses.values():
                assert s["source"] == "user"
                assert s["isModifiable"] is True
                assert s["applyType"] in {"static", "dynamic"}

            # --- Upper-case spec.name: terminal, and the lower-case group is
            # untouched. Redshift stores names in lower case, so without the
            # guard this CR could bind to resource_name's group.
            before = get_aws_parameters(redshift_client, resource_name)
            uc_name = f"{resource_name}-uc"
            uc_data = copy.deepcopy(resource_data)
            uc_data["metadata"]["name"] = uc_name
            uc_data["spec"]["name"] = resource_name.upper()
            uc_data["spec"]["parameterOverrides"] = {STATEMENT_TIMEOUT: timeout_2}
            uc_data["spec"].pop("tags", None)
            uc_ref = k8s.CustomResourceReference(
                CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
                uc_name, namespace="default",
            )
            k8s.create_custom_resource(uc_ref, uc_data)
            try:
                k8s.wait_resource_consumed_by_controller(uc_ref)
                assert k8s.wait_on_condition(
                    uc_ref, "ACK.Terminal", "True",
                    wait_periods=SYNC_WAIT_PERIODS, period_length=SYNC_PERIOD_LENGTH,
                )
                terminal = k8s.get_resource_condition(uc_ref, "ACK.Terminal")
                assert "upper-case" in terminal.get("message", "")
                assert get_aws_parameters(redshift_client, resource_name) == before

                _, deleted = k8s.delete_custom_resource(
                    uc_ref, wait_periods=SYNC_WAIT_PERIODS, period_length=5,
                )
                assert deleted
                assert not k8s.get_resource_exists(uc_ref)
                get_aws_group(redshift_client, resource_name)
            finally:
                if k8s.get_resource_exists(uc_ref):
                    k8s.delete_custom_resource(uc_ref)

            # --- Update parameters: change one, remove the other ---
            # A merge-patch null deletes the map key, which must reset that
            # parameter alone and leave the changed one in place.
            patch_and_assert_synced(ref, {
                "spec": {
                    "parameterOverrides": {
                        STATEMENT_TIMEOUT: timeout_2,
                        USER_ACTIVITY_LOGGING: None,
                    },
                },
            })
            params = get_aws_parameters(redshift_client, resource_name)
            assert params[STATEMENT_TIMEOUT]["ParameterValue"] == timeout_2
            assert params[STATEMENT_TIMEOUT]["Source"] == "user"
            assert params[USER_ACTIVITY_LOGGING]["ParameterValue"] == logging_default
            assert params[USER_ACTIVITY_LOGGING]["Source"] == "engine-default"
            # Without the post-sync requeue this would read the pre-update
            # status (rds-controller#298).
            statuses = status_by_name(k8s.get_resource(ref))
            assert statuses[STATEMENT_TIMEOUT]["parameterValue"] == timeout_2
            assert USER_ACTIVITY_LOGGING not in statuses

            # --- WLM JSON: AWS may re-serialize the body, which would make the
            # string comparison loop instead of settling ---
            assert defaults[WLM_JSON_CONFIGURATION]["IsModifiable"]
            patch_and_assert_synced(ref, {
                "spec": {"parameterOverrides": {WLM_JSON_CONFIGURATION: wlm_value}},
            })
            params = get_aws_parameters(redshift_client, resource_name)
            observed = params[WLM_JSON_CONFIGURATION]["ParameterValue"]
            assert json.loads(observed) == json.loads(wlm_value)
            assert params[WLM_JSON_CONFIGURATION]["Source"] == "user"
            logging.info(
                f"wlm_json_configuration returned byte-identical: {observed == wlm_value}"
            )
            # Every reconcile clears conditions and stamps Synced with the
            # current time, so an unchanged lastTransitionTime means no
            # reconcile ran, i.e. no perpetual diff.
            settled = condition.get_synced_last_transition_time(ref)
            time.sleep(STABILITY_WAIT_SECONDS)
            assert k8s.wait_on_condition(
                ref, "ACK.ResourceSynced", "True", wait_periods=1, period_length=1,
            )
            assert condition.get_synced_last_transition_time(ref) == settled
            statuses = status_by_name(k8s.get_resource(ref))
            assert statuses[WLM_JSON_CONFIGURATION]["source"] == "user"

            # --- Update tags: change, remove and add ---
            patch_and_assert_synced(ref, {
                "spec": {
                    "tags": [
                        {"key": "environment", "value": "staging"},
                        {"key": "owner", "value": "ack"},
                    ],
                },
            })
            group = get_aws_group(redshift_client, resource_name)
            tagutil.assert_equal_without_ack_tags(
                {"environment": "staging", "owner": "ack"},
                {t["Key"]: t["Value"] for t in group.get("Tags", [])},
            )

            # --- Description is create-only: an edit settles without effect ---
            patch_and_assert_synced(ref, {
                "spec": {"description": "an edited description"},
            })
            group = get_aws_group(redshift_client, resource_name)
            assert group["Description"] == description
        finally:
            # --- Delete ---
            k8s.delete_custom_resource(ref)
            time.sleep(DELETE_WAIT_AFTER_SECONDS)

        assert not k8s.get_resource_exists(ref)
        with pytest.raises(ClientError) as exc:
            redshift_client.describe_cluster_parameter_groups(
                ParameterGroupName=resource_name,
            )
        assert exc.value.response["Error"]["Code"] == "ClusterParameterGroupNotFound"
