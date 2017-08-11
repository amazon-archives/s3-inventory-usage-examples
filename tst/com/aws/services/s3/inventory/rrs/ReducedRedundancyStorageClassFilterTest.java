/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test on filtering out the objects not belonging to the "REDUCED_REDUNDANCY" storage class.
 */
public class ReducedRedundancyStorageClassFilterTest {
    ReducedRedundancyStorageClassFilter storageClassFilter;

    @Before
    public void setUp(){
        storageClassFilter = new ReducedRedundancyStorageClassFilter();
    }

    @Test
    public void FilterReducedRedundancyStorageClassSuccess(){
        String bucket = "testBucket";
        String key = "testKey";
        String version = "testVersionId";
        String isLatest = "testIsLatest";
        String isDeleteMaker = "testIsDeleteMaker";
        String size = "testSize";
        String date = "testLastModifiedDate";
        String eTag = "testETag";
        String storage = "REDUCED_REDUNDANCY";
        String multiPartUploaded = "testMultiPartUploaded";
        String status = "testReplicationStatus";
        InventoryReportLine testInventoryReport = this.buildInventoryReport(
                bucket, key, version, isLatest, isDeleteMaker, size,
                date, eTag, storage, multiPartUploaded, status);
        assertThat(storageClassFilter.call(testInventoryReport), is(true));
    }

    @Test
    public void FilterReducedRedundancyStorageClassFail(){
        String bucket = "testBucket";
        String key = "testKey";
        String version = "testVersionId";
        String isLatest = "testIsLatest";
        String isDeleteMaker = "testIsDeleteMaker";
        String size = "testSize";
        String date = "testLastModifiedDate";
        String eTag = "testETag";
        String storage = "testStorage";
        String multiPartUploaded = "testMultiPartUploaded";
        String status = "testReplicationStatus";
        InventoryReportLine testInventoryReport = this.buildInventoryReport(
                bucket, key, version, isLatest, isDeleteMaker, size,
                date, eTag, storage, multiPartUploaded, status);
        assertThat(storageClassFilter.call(testInventoryReport), is(false));
    }

    private InventoryReportLine buildInventoryReport(
            String bucket, String key, String version, String isLatest, String isDeleteMaker, String size,
            String date, String eTag, String storageClass, String multiPartUploaded, String replicationStatus){
        InventoryReportLine line = new InventoryReportLine();
        line.setBucket(bucket);
        line.setKey(key);
        line.setVersionId(version);
        line.setIsLatest(isLatest);
        line.setIsDeleteMaker(isDeleteMaker);
        line.setSize(size);
        line.setLastModifiedDate(date);
        line.seteTag(eTag);
        line.setStorageClass(storageClass);
        line.setMultiPartUploaded(multiPartUploaded);
        line.setReplicationStatus(replicationStatus);
        return line;
    }
}
