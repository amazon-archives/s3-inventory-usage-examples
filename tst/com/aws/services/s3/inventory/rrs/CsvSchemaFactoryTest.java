/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.junit.Test;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertThat;

/**
 * Test on building CsvSchema based on manifestStorage
 */
public class CsvSchemaFactoryTest {
    @Test
    public void buildCsvSchemaBuilderSuccess() {
        String testFileSchema = "Bucket, Key, Versionid";
        InventoryManifest testManifestStorage = new InventoryManifest();
        testManifestStorage.setFileSchema(testFileSchema);

        CsvSchema testCsvSchema = CsvSchemaFactory.buildSchema(testManifestStorage);
        CsvSchema expected = CsvSchema.builder()
                .addColumn("Bucket")
                .addColumn("Key")
                .addColumn("Versionid")
                .build();

        // Since there's no equal method built for CsvSchema class
        // Use samePropertyValuesAs to compare the values of two CsvSchema objects
        assertThat(testCsvSchema, samePropertyValuesAs(expected));
    }
}
