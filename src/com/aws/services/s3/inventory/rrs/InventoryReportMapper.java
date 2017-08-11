/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.List;

/**
 * This InventoryReportMapper class maps each line of the inventory report to a InventoryReportLine POJO.
 */
public class InventoryReportMapper implements FlatMapFunction<List<String>, InventoryReportLine> {
    private final InventoryManifest manifest;

    public InventoryReportMapper(InventoryManifest inventoryManifest){
        this.manifest = inventoryManifest;
    }

    @Override
    public Iterator<InventoryReportLine> call(List<String> inventoryReportLine) throws Exception{
        InventoryReportLineMapper mapper =
                new InventoryReportLineMapper(manifest);
        return mapper.mapInventoryReportLine(inventoryReportLine).iterator();
    }
}
