	// CreateCluster returns no cluster ARN, so build it for syncTags.
	if ko.Spec.Identifier != nil {
		arn := ackv1alpha1.AWSResourceName(rm.clusterARN(*ko.Spec.Identifier))
		ko.Status.ACKResourceMetadata.ARN = &arn
	}
	// CreateCluster reports a KMS key as its ARN; keep the user's form.
	keepEquivalentKMSKeyIDs(&ko.Spec, &desired.ko.Spec)
