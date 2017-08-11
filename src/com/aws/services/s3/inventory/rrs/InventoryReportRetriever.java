/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;

/**
 * This InventoryReportRetriever class gets the inventReport.csv.gz file,
 * verifies its checksum, and transfer it to the String format.
 */
public class InventoryReportRetriever implements Serializable {
    private final AmazonS3 s3Client;
    private InventoryManifest inventoryManifest;
    private InventoryManifest.Locator locator;

    public InventoryReportRetriever(AmazonS3 client, InventoryManifest.Locator locator,
                                    InventoryManifest manifest){
        this.s3Client = client;
        this.locator = locator;
        this.inventoryManifest = manifest;
    }

    /**
     * Get the original inventory report from S3, unzip it, and transfer it into a String format.
     * @return inventReport String
     * @throws IOException when getting object from S3 fails
     * or the checksum of the inventory report and the checksum specified in the manifest file not match
     */
    public String getInventoryReportToString() throws IOException {
        String inventReportKey = locator.getKey();
        String bucketName = inventoryManifest.getSourceBucket();

        try (S3Object s3InventoryReport = s3Client.getObject(
                new GetObjectRequest(bucketName, inventReportKey))) {
            InputStream objectData = s3InventoryReport.getObjectContent();
            byte[] zippedData = IOUtils.toByteArray(objectData);
            String actualChecksum = DigestUtils.md5Hex(zippedData);
            String expectedChecksum = locator.getMD5checksum();
            if (!actualChecksum.equals(expectedChecksum)) {
                throw new ChecksumMismatchException (expectedChecksum, actualChecksum);
            }
            return IOUtils.toString(new GZIPInputStream(new ByteArrayInputStream(zippedData)));
        }
    }
}
