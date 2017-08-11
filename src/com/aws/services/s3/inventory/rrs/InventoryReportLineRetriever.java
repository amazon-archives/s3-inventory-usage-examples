/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This InventoryReportLineRetriever class gets the original inventory report and parse each line of it into a list.
 */
public class InventoryReportLineRetriever implements Function<InventoryManifest.Locator, List<String>> {
    private final Broadcast<CachedS3ClientFactory> s3ClientFactory;
    private final InventoryManifest manifestStorage;

    public InventoryReportLineRetriever(Broadcast<CachedS3ClientFactory> s3ClientFactory,
                                        InventoryManifest manifest) {
        this.s3ClientFactory = s3ClientFactory;
        this.manifestStorage = manifest;
    }

    @Override
    public List<String> call(InventoryManifest.Locator locator) throws IOException {
        InventoryReportRetriever reportRetriever =
                new InventoryReportRetriever(s3ClientFactory.getValue().get(), locator, manifestStorage);
        return Arrays.asList(reportRetriever.getInventoryReportToString().split("\n"));
    }
}
