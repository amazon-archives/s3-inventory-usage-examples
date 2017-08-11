/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.aws.services.s3.inventory.rrs.InventoryManifest.Locator;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * Testing on writing a manifest.json file and sending it to S3
 */
public class ManifestWriterTest {
    @Captor
    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;

    @Mock
    private AmazonS3 mockS3Client;

    @Before
    public void init() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void writeManifestSuccess() throws IOException{
        String testSrcBucket = "testSrcBucket";
        String testDestBucketName = "testDestBucketName";
        String testDestPrefix = "testDestPrefix";

        InventoryManifest testManifest = new InventoryManifest();
        testManifest.setSourceBucket("testSrcBucket");
        testManifest.setDestinationBucket("testDestBucket");
        testManifest.setVersion("testVersion");
        testManifest.setFileSchema("testFileSchema");
        Locator originalLocator = new Locator();
        originalLocator.setKey("testOriginalLocator");
        originalLocator.setSize(0);
        originalLocator.setMD5checksum("testOriginalLocator");
        ArrayList<Locator> originalLocatorList = new ArrayList();
        originalLocatorList.add(originalLocator);
        testManifest.setLocators(originalLocatorList);

        List<Locator> testLocatorList = new ArrayList();
        Locator testLocator1 = new Locator();
        testLocator1.setKey("testKey1");
        testLocator1.setSize(1);
        testLocator1.setMD5checksum("testMD5_1");
        testLocatorList.add(testLocator1);

        Locator testLocator2 = new Locator();
        testLocator2.setKey("testKey2");
        testLocator2.setSize(2);
        testLocator2.setMD5checksum("testMD5_2");
        testLocatorList.add(testLocator2);

        String expectedJson =
                "{\n" +
                "  \"sourceBucket\" : \"testSrcBucket\",\n" +
                "  \"destinationBucket\" : \"testDestBucket\",\n" +
                "  \"version\" : \"testVersion\",\n" +
                "  \"fileFormat\" : \"CSV\",\n" +
                "  \"fileSchema\" : \"testFileSchema\",\n" +
                "  \"files\" : [ {\n" +
                "    \"key\" : \"testKey1\",\n" +
                "    \"size\" : 1,\n" +
                "    \"MD5checksum\" : \"testMD5_1\"\n" +
                "  }, {\n" +
                "    \"key\" : \"testKey2\",\n" +
                "    \"size\" : 2,\n" +
                "    \"MD5checksum\" : \"testMD5_2\"\n" +
                "  } ]\n" +
                "}";

        ManifestWriter testManifestWriter = new ManifestWriter(mockS3Client, testDestBucketName, testDestPrefix,
                testSrcBucket, testManifest);
        when(mockS3Client.putObject(putObjectRequestCaptor.capture())).thenReturn(null);
        testManifestWriter.writeManifest(testLocatorList);
        List<PutObjectRequest> requestList = putObjectRequestCaptor.getAllValues();

        assertThat(requestList.get(0).getBucketName(), is("testDestBucketName"));
        String actualJsonKey = requestList.get(0).getKey();
        List<String> jsonKeyList = Arrays.asList(actualJsonKey.split("\\s*/\\s*"));
        assertThat(jsonKeyList.get(0), is("testDestPrefix"));
        assertThat(jsonKeyList.get(1), is("testSrcBucket"));
        byte[] actualJsonByteArray = IOUtils.toByteArray(requestList.get(0).getInputStream());
        assertThat(actualJsonByteArray, is(expectedJson.getBytes()));
        assertThat((int)requestList.get(0).getMetadata().getContentLength(), is(expectedJson.getBytes().length));

        String expectedChecksum = "6b83fff9e538c82b3d56a65d6a00a80b";
        assertThat(requestList.get(1).getBucketName(), is("testDestBucketName"));

        String actualChecksumKey = requestList.get(1).getKey();
        List<String> checksumKeyList = Arrays.asList(actualChecksumKey.split("\\s*/\\s*"));
        assertThat(checksumKeyList.get(0), is("testDestPrefix"));
        assertThat(checksumKeyList.get(1), is("testSrcBucket"));
        byte[] actualChecksumByteArray = IOUtils.toByteArray(requestList.get(1).getInputStream());
        assertThat(actualChecksumByteArray, is(expectedChecksum.getBytes()));
        assertThat((int)requestList.get(1).getMetadata().getContentLength(), is(expectedChecksum.getBytes().length));
    }
}
