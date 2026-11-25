	// DescribeClusters returns no cluster ARN, so build it for syncTags.
	if ko.Spec.Identifier != nil {
		arn := ackv1alpha1.AWSResourceName(rm.clusterARN(*ko.Spec.Identifier))
		ko.Status.ACKResourceMetadata.ARN = &arn
	}
	// Recover the declared fields Describe reports in another shape.
	if observed := observedCluster(resp.Clusters, ko.Spec.Identifier); observed != nil {
		setObservedSpecFields(ko, observed)
	}
	keepEquivalentKMSKeyIDs(&ko.Spec, &r.ko.Spec)
