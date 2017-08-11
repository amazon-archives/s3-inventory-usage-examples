/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This WriteNewInventoryReportFunc class writes the new inventory report and sends it back to the S3 bucket.
 */
public class WriteNewInventoryReportFunc implements
        FlatMapFunction<Iterator<InventoryReportLine>, InventoryManifest.Locator> {
    private final Broadcast<CachedS3ClientFactory> s3ClientFactory;
    private final InventoryManifest manifestStorage;
    private final String destBucket;
    private final String destPrefix;
    private final String srcBucket;

    public WriteNewInventoryReportFunc(Broadcast<CachedS3ClientFactory> s3ClientFactory, String srcBucket,
                                       InventoryManifest manifestStorage, String destBucket, String destPrefix) {
        this.s3ClientFactory = s3ClientFactory;
        this.manifestStorage = manifestStorage;
        this.srcBucket = srcBucket;
        this.destBucket = destBucket;
        this.destPrefix = destPrefix;
    }

    @Override
    public Iterator<InventoryManifest.Locator> call(Iterator<InventoryReportLine> inventoryReport) throws IOException {
        // Exclude the empty iterators which are caused by the mapPartitions
        // when one partition only owns empty InventoryReportLine iterator after the filtering
        if (!inventoryReport.hasNext()){
            return Collections.emptyIterator();
        }
        List<InventoryReportLine> inventoryReportLineList = IteratorUtils.toList(inventoryReport);
        InventoryReportLineWriter scvWriter = new InventoryReportLineWriter(s3ClientFactory.getValue().get(),
                destBucket, destPrefix, srcBucket, manifestStorage);
        return Collections.singletonList(scvWriter.writeCsvFile(inventoryReportLineList)).iterator();
    }
}
