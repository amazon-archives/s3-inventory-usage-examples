/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.apache.spark.api.java.function.Function;

/**
 * This ReducedRedundancyStorageClassFilter class filters out the objects not belonging to
 * the "REDUCED_REDUNDANCY" storage class.
 */
public class ReducedRedundancyStorageClassFilter implements Function<InventoryReportLine, Boolean> {
    @Override
    public Boolean call(InventoryReportLine inventoryReportLine) {
        return inventoryReportLine.getStorageClass().equals("REDUCED_REDUNDANCY");
    }
}
