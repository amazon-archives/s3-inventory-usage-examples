# **S3 Inventory Usage with Spark and EMR**
Create [Spark](https://spark.apache.org/docs/latest/index.html) applications to analyze the [Amazon S3 Inventory](http://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html) and run on [Amazon EMR](https://aws.amazon.com/emr/).

## Overview
These examples show how to use the **Amazon S3 Inventory** to better manage your S3 storage, by creating a **Spark application** and executing it on **EMR**.

**Amazon Simple Storage Service (S3)** is an object storage built to store and retrieve any amount of data from anywhere. To help manage customers’ storage, S3 generates inventory files on a daily or weekly basis, providing the stored objects' corresponding metadata. Customers could configure what object metadata to in include in the inventory, so it is an alternative to the Amazon S3 synchronous [List API operation](http://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html) and costs half as much (based on 11-AUG-2017 pricing).

Also, **Amazon Elastic MapReduce (EMR)** provides a Hadoop framework to process vast amounts of data across dynamically scalable Amazon EC2 instances. It supports **Apache Spark** which is a distributed processing system, utilizing in-memory caching and optimized execution for big data workloads.

## Motivation
As customers put more and more objects into S3, the S3 Inventory will include tremendous amounts of data. Listing and filtering all the objects locally may take a long time and run into memory issues.

One solution is to create a highly scalable Spark application to operate on the S3 Inventory, such as filtering and generating a new inventory file, because Spark application could easily access S3 data sources, handle the big-data, and run fast on the EMR cluster.

### **Reduced Redundancy Selection Example**
This example is to make a Spark application which filters upon the ["Storage Class"](http://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html) of the inventory file, pulls out all the objects marked with *"REDUCED_REDUNDANCY"*, and generates a new inventory file only including these objects.

##### *Why filter upon the "Storage Class"?*
Currently, there are four types of storage classes for S3 objects and each object has to associate with one class. In most of the regions, *"REDUCED_REDUNDANCY"* class has the lowest durability and the highest [pricing](https://aws.amazon.com/s3/reduced-redundancy/) compared to the other three storage types. Therefore, S3 customers may want to find all their objects in reduced redundancy storage and transfer them to some other storage class.

##### *How does this application work?*
This repository includes a java package named ***S3StorageInventoryExternalExamples***, which can be downloaded and locally built into a ***jar*** file with Maven. Its input is as follows:

**Input:** An argument in the format of "-i,s3://$SOURCE_BUCKET/$INVENTORY'S_YYYY-MM-DDTHH-MMZ,-o,s3://$DESTINATION_BUCKET/$OUTPUT_PREFIX".

Then upload this jar to an S3 bucket and executed it on an EMR cluster to get the following output:

**Output:** A newly generated inventory file in *csv.gz* format and its corresponding *manifest.json*, *manifest.checksum* files. The inventory file will be sent to s3://$DESTINATION_BUCKET/$OUTPUT_PREFIX/$SOURCE_BUCKET/data. Also, the new *manifest.json* and *manifest.checksum* files will be sent to s3://$DESTINATION_BUCKET/$OUTPUT_PREFIX/$SOURCE_BUCKET/$TIME.

#### Dependencies
##### Key Libraries
- [AWS SDK](https://aws.amazon.com/sdk-for-java/): Provide Java APIs for AWS services, such as creating S3 clients and credentials
- [Apache Commons CLI](https://commons.apache.org/proper/commons-cli/index.html): Provide an API for parsing command line options passed to programs.
- [Apache Spark](https://spark.apache.org/docs/latest/streaming-programming-guide.html): Enable scalable, high-throughput, fault-tolerant stream processing of live data streams.
- [Fasterxml jackson-dataformats-text](https://github.com/FasterXML/jackson-dataformats-text/blob/8253f07db96f93e28af0632ab1da47883e798eeb/csv/README.md):
Jackson data format module for reading and writing CSV encoded data.
- [SLF4J](https://www.slf4j.org/): Write the log for error handling.

#### Setup
##### 0. Prerequisites
* [Install the AWS CLI](https://aws.amazon.com/cli/)

* [Enable S3 Inventory](http://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html#storage-inventory-how-to-set-up)
>It may take up to 48 hours to deliver the first inventory file.

* [Install Maven](https://maven.apache.org/install.html)

##### 1. Build a jar file with Maven
Clone this repository and cd to the ./src. Then run the following command.
```
$ mvn package
```
If built successfully, the target jar file
*S3StorageInventoryExternalExamples-1.0-jar-with-dependencies* will be in the ./target folder.

##### 2. Upload the jar to S3
```
// Log in your AWS account
$ aws configure        

// Put the jar into S3 bucket
$ aws s3 cp $JAR_LOCAL_PATH $S3URI
```
###### Examples
```
$ aws s3 cp .../S3StorageInventoryExternalExamples-1.0.jar s3://example.bucket/S3StorageInventoryExternalExamples-1.0.jar
```
Alternatively, use the **Amazon S3 Console**: see [How Do I Upload Files and Folders to an S3 Bucket?](http://docs.aws.amazon.com/AmazonS3/latest/user-guide/upload-objects.html) in the *Amazon Simple Storage Service Console User Guide*.

##### \*3. Create an EMR cluster with Spark installed (optional)
If you haven't run an EMR cluster before, you may need to create one.
```
$ aws emr create-cluster --release-label emr-5.3.1 --applications Name=Spark --ec2-attributes KeyName=myKey --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge --auto-terminate
```
Alternatively, use the Amazon S3 Console: see [Launch Your Sample Amazon EMR Cluster
](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html) in the *Amazon EMR Release Guide*.

##### 4. Run it on EMR
Add a Spark App to a running EMR cluster by running the following command line.
```
$ aws emr add-steps --cluster-id $CLUSTER_ID --steps Type=Spark,Name=“InventoryExternalExample”,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--class,com.aws.services.s3.inventory.rrs.ReducedRedundancyLocatorExampleMain,s3://$BUCKET_OF_THE_JAR/$KEY_OF_THE_JAR,-i,s3://$SOURCE_BUCKET/$INVENTORY_KEY-o,s3://$DESTINATION_BUCKET/$OUTPUT_PREFIX]
```
###### Examples
```
$ aws emr add-steps --cluster-id j-2AXXXXXXGAPLF --steps Type=Spark,Name=“InventoryExternalExample”,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--class,com.aws.services.s3.inventory.rrs.ReducedRedundancyLocatorExampleMain,s3://example.bucket/S3StorageInventoryExternalExamples-1.0.jar,-i,s3://inventory/dest//2017-08-04T12-00Z,-o,s3://inventory/FilteredInventoryReport]
```
Alternatively, use the Amazon S3 Console: see [Adding a Spark Step](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html) in the *Amazon EMR Release Guide*.

For more information about submitting a Spark application to EMR, see the following official documentation.
>http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html

##### \*5. Terminate the EMR cluster (optional)
When the EMR step completes and there is no more step to add, you may terminate the cluster by running the following command line.
```
$ aws emr terminate-clusters --cluster-ids $CLUSTER_ID
```
###### Examples
```
$ aws emr terminate-clusters --cluster-ids j-2AXXXXXXGAPLF
```
Alternatively, use the Amazon S3 Console: see [Terminate a Cluster](http://docs.aws.amazon.com/emr/latest/ManagementGuide/UsingEMR_TerminateJobFlow.html) in the Amazon EMR Documentation.

### Future Improvements
- Find the age of each object by filtering upon "LastModifiedDate".
- Analyze the bucket storage usage by filtering upon objects' extensions.

#### Copyright
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
