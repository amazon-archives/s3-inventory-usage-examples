/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test on accessing Amazon S3 instances on Spark workers
 */
public class CachedS3ClientFactoryTest {
    CachedS3ClientFactory cachedS3ClientFactory;

    @Before
    public void setUp(){
        cachedS3ClientFactory = new CachedS3ClientFactory();
    }

    @Test
    public void getAmazonS3Success(){
        assertThat(cachedS3ClientFactory.get().getS3AccountOwner().getDisplayName(),
                is("S3-StorageInventoryExternalExamples-Test"));
    }
}
