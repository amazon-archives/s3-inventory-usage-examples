/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

/**
 * This InventoryReportLineWriter class writes InventoryReportLine objects to a new CSV file
 * and sends it to the S3 bucket
 */
public class InventoryReportLineWriter implements Serializable {
    private final AmazonS3 s3Client;
    private final String bucketName;
    private String outputInventoryReportKey;
    private CsvSchema schema;

    public InventoryReportLineWriter(AmazonS3 client, String destBucketName, String destPrefix,
                                     String srcBucket, InventoryManifest inventoryManifest) throws IOException{
        this.s3Client = client;
        this.bucketName = destBucketName;
        String uuid = UUID.randomUUID().toString();
        this.outputInventoryReportKey = destPrefix + "/" + srcBucket + "/data/" + uuid + ".csv.gz";
        this.schema = CsvSchemaFactory.buildSchema(inventoryManifest);
    }

    /**
     * Write a new inventory report to S3 and returns a locator which includes this inventory report's information
     * @return Locator which includes the information of this new report
     * @throws IOException thrown when GZIPOutputStream not created successfully or csvMapper.write() fails
     */
    public InventoryManifest.Locator writeCsvFile(List<InventoryReportLine> inventoryReportLine) throws IOException{
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.enable(JsonGenerator.Feature.IGNORE_UNKNOWN);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        csvMapper.writer(schema).writeValues(gzipOutputStream).writeAll(inventoryReportLine).close();
        byte[] zipByteArray = byteArrayOutputStream.toByteArray();

        InputStream zipInputStream = new ByteArrayInputStream(zipByteArray);
        ObjectMetadata metaData = new ObjectMetadata();
        metaData.setContentLength(zipByteArray.length);
        PutObjectRequest request = new PutObjectRequest(bucketName, outputInventoryReportKey, zipInputStream, metaData);
        s3Client.putObject(request);

        return this.buildLocator(zipByteArray);
    }

    /**
     * Helper function, which creates a new Locator
     * @param outputInventoryBytes which is byte array
     * @return Locator which includes the information of the CSV file
     */
    private InventoryManifest.Locator buildLocator(byte[] outputInventoryBytes){
        InventoryManifest.Locator locator = new InventoryManifest.Locator();
        locator.setKey(outputInventoryReportKey);
        locator.setSize(outputInventoryBytes.length);
        locator.setMD5checksum(DigestUtils.md5Hex(outputInventoryBytes));
        return locator;
    }
}
