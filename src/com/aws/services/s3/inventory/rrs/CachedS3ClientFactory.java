/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * This CachedS3ClientFactory class provides a serializable wrapper
 * through which to access Amazon S3 instances on Spark workers.
 */
public class CachedS3ClientFactory implements SerializableSupplier<AmazonS3> {
    private transient volatile AmazonS3 client;

    @Override
    public AmazonS3 get() {
        if (client == null) {
            client = new AmazonS3Client();
        }
        return client;
    }
}