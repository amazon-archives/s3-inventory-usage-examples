/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3URI;

/**
 * This BucketKey class takes the S3 URIs of the source bucket and the output bucket,
 * parses the input URI into scrBucket and scrKey, and parses the output URI into destBucket and destPrefix.
 */
public class BucketKey {

    private final String srcBucket;
    private final String srcKey;
    private final String destBucket;
    private final String destPrefix;

    public BucketKey(String inputFilePath, String outputFilePath){
        AmazonS3URI srcURI = new AmazonS3URI(inputFilePath);
        AmazonS3URI destURI = new AmazonS3URI(outputFilePath);
        this.srcBucket = srcURI.getBucket();
        this.srcKey = srcURI.getKey();
        this.destBucket = destURI.getBucket();
        this.destPrefix = destURI.getKey();
    }

    public String getSrcBucket() {
        return srcBucket;
    }

    public String getSrcKey() {
        return srcKey;
    }

    public String getDestBucket() {
        return destBucket;
    }

    public String getDestPrefix() {
        return destPrefix;
    }
}
