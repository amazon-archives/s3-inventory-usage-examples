/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.Arrays;
import java.util.List;

/**
 * This CsvSchemaFactory class gets the schemaFile field of an inventory manifest object,
 * and defines a new CSV Schema including each element of the schemaFile.
 */
public class CsvSchemaFactory {
    /**
     * Build a CSV schema according to the content of the fileSchema in the manifest file
     * @param inventoryManifest the original manifest of the inventory report
     * @return CsvSchema
     */
    public static CsvSchema buildSchema(InventoryManifest inventoryManifest){
        List<String> columnList = Arrays.asList(
                inventoryManifest.getFileSchema().split("\\s*,\\s*"));
        CsvSchema.Builder schemaBuilder = new CsvSchema.Builder();
        for (String eachColumn: columnList) {
            schemaBuilder.addColumn(eachColumn);
        }
        return schemaBuilder.build();
    }
}
