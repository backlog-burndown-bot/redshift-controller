	// Redshift stores group names in lower case, so an upper-case spec.name
	// can never own a group; report NotFound rather than risk binding to the
	// lower-case group of the same name.
	if hasUpperCase(r.ko.Spec.Name) {
		return nil, ackerr.NotFound
	}
