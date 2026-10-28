	// CreateClusterParameterGroup takes no parameters, so apply the declared
	// overrides now rather than leaving the group Synced with defaults until
	// the next resync.
	if err = rm.syncParameters(ctx, desired, nil); err != nil {
		return nil, err
	}
