/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * This ManifestWriter class creates a new manifest.json file and writes it back to S3
 */
public class ManifestWriter {
    private final AmazonS3 s3Client;
    private final String bucketName;
    private final InventoryManifest originalManifest;
    private String manifestKey;
    private String checksumKey;

    public ManifestWriter(AmazonS3 client, String destBucketName, String destPrefix, String srcBucket,
                          InventoryManifest originalManifest){
        this.s3Client = client;
        this.bucketName = destBucketName;
        String time = this.getTime();
        this.manifestKey = destPrefix + "/" + srcBucket + "/" + time + "/manifest.json";
        this.checksumKey = destPrefix + "/" + srcBucket + "/" + time + "/manifest.checksum";
        this.originalManifest = originalManifest;
    }

    /**
     * Write manifest.json and manifest.checksum to S3
     * @throws IOException thrown when IOUtils.toInputStream fails or ObjectMapper.write() fails
     */
    public void writeManifest(List<InventoryManifest.Locator> locatorList) throws IOException{
        InventoryManifest manifest = new InventoryManifest();
        manifest.setSourceBucket(originalManifest.getSourceBucket());
        manifest.setDestinationBucket(originalManifest.getDestinationBucket());
        manifest.setVersion(originalManifest.getVersion());
        manifest.setFileFormat("CSV");
        manifest.setFileSchema(originalManifest.getFileSchema());
        manifest.setLocators(locatorList);

        // Write back manifest.json file to S3
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest);
        byte[] bytesJson = json.getBytes();
        ObjectMetadata jsonMetaData = new ObjectMetadata();
        jsonMetaData.setContentLength(bytesJson.length);
        try (InputStream inputStream = IOUtils.toInputStream(json, "UTF-8")){
            PutObjectRequest request = new PutObjectRequest(bucketName, manifestKey, inputStream, jsonMetaData);
            s3Client.putObject(request);
        }

        // Write back manifest.checksum file to S3
        String checksum = DigestUtils.md5Hex(json);
        byte[] bytesChecksum = checksum.getBytes();
        ObjectMetadata checksumMetaData = new ObjectMetadata();
        checksumMetaData.setContentLength(bytesChecksum.length);
        try (InputStream inputStream = IOUtils.toInputStream(checksum, "UTF-8")){
            PutObjectRequest request = new PutObjectRequest(bucketName, checksumKey, inputStream, checksumMetaData);
            s3Client.putObject(request);
        }
    }

    /**
     * A helper function which gets the local time
     * @return String in "yyyy-MM-dd'T'HH-mm'Z'" format
     */
    private String getTime(){
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("GTM"));
        return sdf.format(now);
    }
}
