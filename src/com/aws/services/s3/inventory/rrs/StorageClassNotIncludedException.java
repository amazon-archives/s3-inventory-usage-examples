/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

/**
 * The StorageClassNotIncludedException is thrown when
 * the "fileSchema" in manifest.json file does not include "StorageClass",
 * which means the inventory report is missing the Storage Class information.
 */
public class StorageClassNotIncludedException extends RuntimeException{
    public StorageClassNotIncludedException() {
        super("Storage class NOT found in the inventory report");
    }
}
