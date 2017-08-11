/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * This InventoryManifestRetriever class gets manifest.json and manifest.checksum files from S3,
 * converts them to POJOs, and checks if the manifest.checksum matches the MD5 of the manifest.json file.
 */
public class InventoryManifestRetriever {
    private final AmazonS3 s3Client;
    private final String bucketName;
    private String bucketKeyJson;
    private String bucketKeyChecksum;
    private ObjectMapper mapper;


    public InventoryManifestRetriever(AmazonS3 client, String bucket, String key) throws IOException {
        this.s3Client = client;
        this.bucketName = bucket;
        this.bucketKeyJson = key + "/manifest.json";
        this.bucketKeyChecksum = key + "/manifest.checksum";
        this.mapper = new ObjectMapper();
    }

    /**
     * Transfer the InputStream to a String representation of the Inventory Report Manifest in JSON format
     * @param is InputStream of the object from S3
     * @return String, which is a String representation of the object
     */
    private String inputStreamToString(InputStream is) throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(is, writer);
        return writer.toString();
    }

    /**
     * Check if the MD5s of manifest.json and manifest.checksum equal
     * if so, pull out the manifest file and map it into a POJO
     * @return inventoryManifestStorage InventoryManifest, which stores all the elements of the manifest.json file
     */
    public InventoryManifest getInventoryManifest() throws Exception {
        // Get manifest.json and transfer it to String
        GetObjectRequest requestJson = new GetObjectRequest(bucketName, bucketKeyJson);
        S3Object jsonObject = s3Client.getObject(requestJson);
        String jsonFile = inputStreamToString(jsonObject.getObjectContent());
        jsonObject.close();

        // Get manifest.checksum and transfer it to String with no whitespace
        GetObjectRequest requestChecksum = new GetObjectRequest(bucketName, bucketKeyChecksum);
        S3Object checksumObject = s3Client.getObject(requestChecksum);
        String expectedChecksum = inputStreamToString(checksumObject.getObjectContent())
                .replaceAll("\\s","");
        checksumObject.close();

        // Compare manifest.json and manifest.checksum's MD5 value
        String actualChecksum = DigestUtils.md5Hex(jsonFile);
        if (!actualChecksum.equals(expectedChecksum)) {
            throw new ChecksumMismatchException (expectedChecksum, actualChecksum);
        }

        return mapper.readValue(jsonFile, InventoryManifest.class);
    }
}
