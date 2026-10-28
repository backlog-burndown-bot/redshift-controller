	// DescribeClusterParameterGroups does not return parameter values, so read
	// them with DescribeClusterParameters.
	overrides, statuses, err := rm.getParameterOverrides(ctx, ko.Spec.Name, r.ko.Spec.ParameterOverrides)
	if err != nil {
		return nil, err
	}
	ko.Spec.ParameterOverrides = overrides
	ko.Status.ParameterOverrideStatuses = statuses
