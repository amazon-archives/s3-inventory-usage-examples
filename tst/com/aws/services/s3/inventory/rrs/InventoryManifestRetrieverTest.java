/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.aws.services.s3.inventory.rrs.InventoryManifest.Locator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * Test on obtaining manifest files and mapping to POJOs.
 */
public class InventoryManifestRetrieverTest {
    private final String testBucketName = "testBucketName";
    private final String testBucketKey = "testBucketKey";

    @Captor
    ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor;

    private ObjectMapper objectMapper;
    private InventoryManifestRetriever retriever;

    @Mock
    private AmazonS3 mockS3Client;

    @Mock
    private S3Object mockS3JsonObject;

    @Mock
    private S3Object mockS3ChecksumObject;

    @Before
    public void init() throws IOException {
        MockitoAnnotations.initMocks(this);
        objectMapper = new ObjectMapper();
        retriever = new InventoryManifestRetriever(mockS3Client, testBucketName, testBucketKey);
    }

    @Test
    public void getInventoryManifestSuccess() throws Exception {
        InventoryManifest expectedManifest = manifest();
        byte[] expectedManifestBytes = manifestBytes(expectedManifest);
        when(mockS3JsonObject.getObjectContent()).thenReturn(new S3ObjectInputStream(
                new ByteArrayInputStream(expectedManifestBytes), null));

        String expectedChecksum = "a6121a6a788be627a68d7e9def9f6968";
        byte[] expectedChecksumBytes = expectedChecksum.getBytes(StandardCharsets.UTF_8);
        when(mockS3ChecksumObject.getObjectContent()).thenReturn(new S3ObjectInputStream(
                new ByteArrayInputStream(expectedChecksumBytes), null));

        when(mockS3Client.getObject(getObjectRequestCaptor.capture()))
                .thenReturn(mockS3JsonObject)
                .thenReturn(mockS3ChecksumObject);
        InventoryManifest result = retriever.getInventoryManifest();
        assertThat(result, is(expectedManifest));

        List<GetObjectRequest> request = getObjectRequestCaptor.getAllValues();
        assertThat(request.get(0).getBucketName(), is("testBucketName"));
        assertThat(request.get(0).getKey(), is("testBucketKey/manifest.json"));
        assertThat(request.get(1).getBucketName(), is("testBucketName"));
        assertThat(request.get(1).getKey(), is("testBucketKey/manifest.checksum"));
    }

    @Test (expected = ChecksumMismatchException.class)
    public void getInventoryManifestMD5Mismatch() throws Exception {
        InventoryManifest expectedManifest = manifest();
        byte[] expectedManifestBytes = manifestBytes(expectedManifest);
        byte[] errorBytes = "ERROR".getBytes();
        byte[] wrongManifestBytes = ArrayUtils.addAll(expectedManifestBytes, errorBytes);
        when(mockS3JsonObject.getObjectContent()).thenReturn(new S3ObjectInputStream(
                new ByteArrayInputStream(wrongManifestBytes), null));
        String expectedChecksum = "37289f10a76751046658f6c5e0ab41d9";

        byte[] expectedChecksumBytes = expectedChecksum.getBytes(StandardCharsets.UTF_8);
        when(mockS3ChecksumObject.getObjectContent()).thenReturn(new S3ObjectInputStream(
                new ByteArrayInputStream(expectedChecksumBytes), null));
        when(mockS3Client.getObject(getObjectRequestCaptor.capture())).
                thenReturn(mockS3JsonObject)
                .thenReturn(mockS3ChecksumObject);
        retriever.getInventoryManifest();
    }

    /**
     * Build a sample InventoryManifest object for testing purpose
     */
    private InventoryManifest manifest() throws Exception {
        InventoryManifest testManifest = new InventoryManifest();
        testManifest.setSourceBucket("testSrc");
        testManifest.setDestinationBucket("testDest");
        testManifest.setVersion("testVersion");
        testManifest.setFileFormat("testFormat");
        testManifest.setFileSchema("testSchema");
        Locator testLocator = new Locator();
        testLocator.setKey("testInventReportKey");
        testLocator.setSize(0);
        testLocator.setMD5checksum("testMD5Checksum");
        testManifest.setLocators(Arrays.asList(testLocator));
        return testManifest;
    }

    /**
     * Helper function, which trans manifest object into byte array
     * @param manifest
     * @return byte[]
     * @throws IOException
     */
    private byte[] manifestBytes(InventoryManifest manifest) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        objectMapper.writeValue(byteArrayOutputStream, manifest);
        return byteArrayOutputStream.toByteArray();
    }
}
