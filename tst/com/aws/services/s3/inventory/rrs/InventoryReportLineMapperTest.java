/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test on parsing lines of the inventory report into POJOs
 */
public class InventoryReportLineMapperTest {
    private List<String> testLines;
    private InventoryManifest testManifest;
    private InventoryReportLineMapper mapper;
    private List<InventoryReportLine> testInventoryReportLine;
    private List<InventoryReportLine> expectedInventoryReportLine;

    @Before
    public void init(){
        testManifest = new InventoryManifest();
        testLines = new ArrayList();
        expectedInventoryReportLine = new ArrayList();
    }

    @Test
    public void mapInventoryReportLineSuccess() throws Exception{
        testManifest.setFileSchema("Bucket, Key, Versionid, IsLatest, IsDeleteMaker, Size, LastModifiedDate," +
                "ETag, StorageClass, IsMultipartUploaded, ReplicationStatus");

        String bucket1 = "testBucket1";
        String key1 = "testKey1";
        String version1 = "testVersionId1";
        String isLatest1 = "testIsLatest1";
        String isDeleteMaker1 = "testIsDeleteMaker1";
        String size1 = "testSize1";
        String date1 = "testLastModifiedDate1";
        String eTag1 = "testETag1";
        String storage1 = "testStorage1";
        String multiPartUploaded1 = "testMultiPartUploaded1";
        String status1 = "testReplicationStatus1";
        String inventoryReportLine1 = testInventoryReportLine(bucket1, key1, version1, isLatest1, isDeleteMaker1, size1,
                date1, eTag1, storage1, multiPartUploaded1, status1);
        testLines.add(inventoryReportLine1);

        String bucket2 = "testBucket2";
        String key2 = "testKey2";
        String version2 = "testVersionId2";
        String isLatest2 = "testIsLatest2";
        String isDeleteMaker2 = "testIsDeleteMaker2";
        String size2 = "testSize2";
        String date2 = "testLastModifiedDate2";
        String eTag2 = "testETag2";
        String storage2 = "testStorage2";
        String multiPartUploaded2 = "testMultiPartUploaded2";
        String status2 = "testReplicationStatus2";
        String inventoryReportLine2 = testInventoryReportLine(bucket2, key2, version2, isLatest2, isDeleteMaker2, size2,
                date2, eTag2, storage2, multiPartUploaded2, status2);
        testLines.add(inventoryReportLine2);

        mapper = new InventoryReportLineMapper(testManifest);
        testInventoryReportLine = mapper.mapInventoryReportLine(testLines);

        InventoryReportLine expectedInventoryReport1 = this.buildInventoryReport(
                bucket1, key1, version1, isLatest1, isDeleteMaker1, size1,
                date1, eTag1, storage1, multiPartUploaded1, status1);
        expectedInventoryReportLine.add(expectedInventoryReport1);
        InventoryReportLine expectedInventoryReport2 = this.buildInventoryReport(
                bucket2, key2, version2, isLatest2, isDeleteMaker2, size2,
                date2, eTag2, storage2, multiPartUploaded2, status2);
        expectedInventoryReportLine.add(expectedInventoryReport2);

        assertThat(testInventoryReportLine, is(expectedInventoryReportLine));
    }

    @Test (expected = IOException.class)
    public void mapInventoryReportLineSchemaMismatch() throws Exception{
        testManifest.setFileSchema("Bucket, Key, Size");
        testLines.add("testBucket1, testKey1, testSize1, testStorage1");
        mapper = new InventoryReportLineMapper(testManifest);
        testInventoryReportLine = mapper.mapInventoryReportLine(testLines);
    }

    private String testInventoryReportLine(
            String bucket, String key, String version, String isLatest, String isDeleteMaker, String size,
            String date, String eTag, String storage, String multiPartUploaded, String status) {
        return bucket + "," + key + "," + version + "," + isLatest + "," + isDeleteMaker + "," + size + "," +
                date + "," + eTag + ","+ storage + ","+ multiPartUploaded + "," + status;
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
