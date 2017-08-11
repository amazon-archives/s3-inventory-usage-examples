/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * Test on obtaining the inventory report file, verify its checksum,
 * check if "Storage class" included, and convert it to String format.
 */
public class InventoryReportRetrieverTest {
    private final String testMD5 = "38fe5f041f0c3d3fe81c4b6e24b149f3";

    @Captor
    ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor;

    private InventoryManifest.Locator testLocator;
    private InventoryManifest testManifest;
    private InventoryReportRetriever reportRetriever;

    @Mock
    private AmazonS3 mockS3Client;

    @Mock
    private S3Object mockS3Object;

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        testLocator = testLocator();
        testManifest = testManifest();
    }

    @Test
    public void getInventReportSuccess() throws Exception {
        testLocator.setMD5checksum(testMD5);
        testManifest.setFileSchema("storageClass, size");
        reportRetriever = new InventoryReportRetriever(mockS3Client, testLocator, testManifest);

        String expectedInventoryReportString = "testString";
        byte[] expectedInventoryReportBytes = inventReportBytes(expectedInventoryReportString);
        when(mockS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(
                new ByteArrayInputStream(expectedInventoryReportBytes), null));
        when(mockS3Client.getObject(getObjectRequestCaptor.capture())).thenReturn(mockS3Object);

        String result = reportRetriever.getInventoryReportToString();
        assertThat(result, is(expectedInventoryReportString));

        GetObjectRequest request = getObjectRequestCaptor.getValue();
        assertThat(request.getBucketName(), is("testBucket"));
        assertThat(request.getKey(), is("testInventReportKey"));
    }

    @Test (expected = ChecksumMismatchException.class)
    public void getInventReportMD5Mismatch() throws Exception {
        testLocator.setMD5checksum("badChecksum");
        testManifest.setFileSchema("storageClass, size");
        reportRetriever = new InventoryReportRetriever(mockS3Client, testLocator, testManifest);

        String expectedInventoryReportString = "testString";
        byte[] expectedInventReportBytes = inventReportBytes(expectedInventoryReportString);
        when(mockS3Object.getObjectContent()).thenReturn(new S3ObjectInputStream(
                new ByteArrayInputStream(expectedInventReportBytes), null));
        when(mockS3Client.getObject(getObjectRequestCaptor.capture())).thenReturn(mockS3Object);
        reportRetriever.getInventoryReportToString();
    }

    private InventoryManifest.Locator testLocator() {
        InventoryManifest.Locator testLocator = new InventoryManifest.Locator();
        testLocator.setKey("testInventReportKey");
        testLocator.setSize(0);
        testLocator.setMD5checksum("testMD5checksum");
        return testLocator;
    }

    private InventoryManifest testManifest() throws Exception {
        InventoryManifest testManifest = new InventoryManifest();
        testManifest.setSourceBucket("testBucket");
        testManifest.setDestinationBucket("testDest");
        testManifest.setVersion("testVersion");
        testManifest.setFileFormat("testFormat");
        testManifest.setFileSchema("testFileSchema");
        InventoryManifest.Locator testLocator = this.testLocator();
        testManifest.setLocators(Arrays.asList(testLocator));
        return testManifest;
    }

    /**
     * Helper function, which compresses String and converts it to byte array
     * @param inventReport String
     * @return byte[]
     * @throws Exception
     */
    private byte[] inventReportBytes(String inventReport) throws Exception {
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(inventReport.getBytes("UTF-8"));
        gzip.close();
        return obj.toByteArray();
    }
}
