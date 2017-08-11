/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * Test on generating a new inventory report and sending it to S3
 */
public class InventoryReportLineWriterTest {
    @Captor
    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;

    @Mock
    private AmazonS3 mockS3Client;

    @Before
    public void init() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void writeObjectToCsvSuccess() throws IOException{
        String testSrcBucket = "testSrcBucket";
        String testDestBucketName = "testDestBucketName";
        String testDestPrefix = "testDestPrefix";
        String testFileSchema = "Bucket, Key, Versionid, IsLatest, IsDeleteMaker, Size, LastModifiedDate," +
                "ETag, StorageClass, IsMultipartUploaded, ReplicationStatus";
        InventoryManifest testInventoryManifest = buildInventoryManifest(testFileSchema);
        List<InventoryReportLine> testInventoryReportLine = buildInventoryReportStorgaeList();
        InventoryReportLineWriter testCsvWriter = new InventoryReportLineWriter(mockS3Client, testDestBucketName, testDestPrefix,
                testSrcBucket, testInventoryManifest);
        when(mockS3Client.putObject(putObjectRequestCaptor.capture())).thenReturn(null);
        InventoryManifest.Locator testLocator = testCsvWriter.writeCsvFile(testInventoryReportLine);
        PutObjectRequest request = putObjectRequestCaptor.getValue();

        // Test if bucketName and outputInventoryReportKey match in args
        assertThat(request.getBucketName(), is("testDestBucketName"));
        String actualKey = request.getKey();
        List<String> keyList = Arrays.asList(actualKey.split("\\s*/\\s*"));
        assertThat(keyList.get(0), is("testDestPrefix"));
        assertThat(keyList.get(1), is("testSrcBucket"));
        assertThat(keyList.get(2), is("data"));

        // Test if the inputStream match when put object to S3
        byte[] actualByteArray = IOUtils.toByteArray(request.getInputStream());
        String actualInventoryReportString = IOUtils.toString(new GZIPInputStream(
                new ByteArrayInputStream(actualByteArray)));
        String expectedInventoryReportString =
                "testBucket1,testKey1,testVersionId1,testIsLatest1,testIsDeleteMaker1,testSize1,testLastModifiedDate1," +
                "testETag1,testStorage1,testMultiPartUploaded1,testReplicationStatus1\n" +
                "testBucket2,testKey2,testVersionId2,testIsLatest2,testIsDeleteMaker2,testSize2,testLastModifiedDate2," +
                "testETag2,testStorage2,testMultiPartUploaded2,testReplicationStatus2\n";
        assertThat(actualInventoryReportString, is(expectedInventoryReportString));

        // Test if the metaData match when put object to S3
        long expectedMetaDataLength = actualByteArray.length;
        assertThat(request.getMetadata().getContentLength(), is(expectedMetaDataLength));

        // Test if the return result of writeCsvFile() match the expected one
        InventoryManifest.Locator expectedLocator = new InventoryManifest.Locator();
        expectedLocator.setKey(actualKey);
        long expectedSize = actualByteArray.length;
        expectedLocator.setSize(expectedSize);
        String expectedChecksum = DigestUtils.md5Hex(actualByteArray);
        expectedLocator.setMD5checksum(expectedChecksum);
        assertThat(testLocator, is(expectedLocator));
    }

    private InventoryManifest buildInventoryManifest(String testFileSchema){
        InventoryManifest testManifestStorage = new InventoryManifest();
        testManifestStorage.setSourceBucket("testSrcBucket");
        testManifestStorage.setDestinationBucket("testDestBucket");
        testManifestStorage.setVersion("testVersion");
        testManifestStorage.setFileSchema(testFileSchema);
        InventoryManifest.Locator originalLocator = new InventoryManifest.Locator();
        originalLocator.setKey("testOriginalLocator");
        originalLocator.setSize(0);
        originalLocator.setMD5checksum("testOriginalLocator");
        ArrayList<InventoryManifest.Locator> originalLocatorList = new ArrayList();
        originalLocatorList.add(originalLocator);
        testManifestStorage.setLocators(originalLocatorList);
        return testManifestStorage;
    }

    private List<InventoryReportLine> buildInventoryReportStorgaeList(){
        List<InventoryReportLine> testReportStorageList = new ArrayList();
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
        InventoryReportLine inventoryReport1 = buildInventoryReport(bucket1, key1, version1, isLatest1,
                isDeleteMaker1, size1, date1, eTag1, storage1, multiPartUploaded1, status1);
        testReportStorageList.add(inventoryReport1);

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
        InventoryReportLine inventoryReport2 = buildInventoryReport(bucket2, key2, version2, isLatest2,
                isDeleteMaker2, size2, date2, eTag2, storage2, multiPartUploaded2, status2);
        testReportStorageList.add(inventoryReport2);

        return testReportStorageList;
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
