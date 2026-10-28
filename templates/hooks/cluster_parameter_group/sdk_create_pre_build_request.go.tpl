	if err = validateParameterGroupName(desired.ko.Spec.Name); err != nil {
		return nil, err
	}
