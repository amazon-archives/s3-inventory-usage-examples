/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This ReducedRedundancyLocatorExampleMain class writes new filtered inventory report and its manifest files,
 * and send them to the S3 bucket specified by the args.
 */
public class ReducedRedundancyLocatorExampleMain {
    private static final Logger LOG = LoggerFactory.getLogger(ReducedRedundancyLocatorExampleMain.class);
    private static final String PARSE_ERROR_MSG = "usage: parse args\n" +
            " -i <s3://source-bucket/YYYY-MM-DDTHH-MMZ> \n"
            + " -o <s3://destination-bucket/output-prefix> \n";

    public static void main(String[] args) throws Exception{
        String srcBucketName;
        String scrBucketKey;
        String destBucketName;
        String destPrefix;
        ArgumentParser argumentParser = new ArgumentParser();
        AmazonS3 s3Client = new AmazonS3Client();

        try {
            BucketKey location = argumentParser.parseArguments(args);
            srcBucketName = location.getSrcBucket();
            scrBucketKey = location.getSrcKey();
            destBucketName = location.getDestBucket();
            destPrefix = location.getDestPrefix();
        } catch (ParseException e) {
            LOG.info(PARSE_ERROR_MSG);
            throw new IllegalArgumentException("Parser throw a parse Exception", e);
        }

        // Obtain the original manifest files
        InventoryManifestRetriever inventoryManifestRetriever =
                new InventoryManifestRetriever(s3Client, srcBucketName, scrBucketKey);
        InventoryManifest manifest = inventoryManifestRetriever.getInventoryManifest();

        // Check if the inventory report includes the StorageClass column
        String fileSchema = manifest.getFileSchema();
        String filterColumn = "storageClass";
        if (!StringUtils.containsIgnoreCase(fileSchema, filterColumn)) {
            throw new StorageClassNotIncludedException();
        }

        //Create Spark Context
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Broadcast<CachedS3ClientFactory> clientFactory = sc.broadcast(new CachedS3ClientFactory());

        // Get the inventory report, split it into lines, parse each line to a POJO,
        // Filter, and write new csv file to S3
        JavaRDD<InventoryManifest.Locator> locatorRDD = sc.parallelize(manifest.getLocators());
        List<InventoryManifest.Locator> newLocatorList = locatorRDD
                .map(new InventoryReportLineRetriever(clientFactory, manifest))
                .flatMap(new InventoryReportMapper(manifest))
                .filter(new ReducedRedundancyStorageClassFilter())
                .mapPartitions(new WriteNewInventoryReportFunc(clientFactory, srcBucketName, manifest,
                        destBucketName, destPrefix))
                .collect();

        // Generate new manifest files including new locators, and send them back to S3
        new ManifestWriter(s3Client, destBucketName, destPrefix, srcBucketName, manifest)
                .writeManifest(newLocatorList);

        sc.close();
    }
}
