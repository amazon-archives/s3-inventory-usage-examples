/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.List;

/**
 * This InventoryManifest class stores all the elements of a manifest.json file
 * and provides a constructor corresponding to map them into POJOs
 */
public class InventoryManifest implements Serializable {

    /**
     * The name of the source bucket.
     */
    private String sourceBucket;

    /**
     * The name of the destination bucket.
     */
    private String destinationBucket;

    /**
     * The version of the inventory list.
     */
    private String version;

    /**
     * The format of the inventory file.
     */
    private String fileFormat;

    /**
     * The schema of the inventory file.
     */
    private String fileSchema;

    /**
     * The actual list of the inventory file.
     */
    @JsonProperty("files")
    private List<Locator> locators;

    // Getters and setters
    @JsonProperty("files")
    public List<Locator> getLocators() {
        return locators;
    }

    @JsonProperty("files")
    public void setLocators(List<Locator> locators) {
        this.locators = locators;
    }

    public String getFileSchema() {
        return fileSchema;
    }

    public void setFileSchema(String fileSchema) {
        this.fileSchema = fileSchema;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
    }

    public String getSourceBucket() {
        return sourceBucket;
    }

    public void setSourceBucket(String sourceBucket) {
        this.sourceBucket = sourceBucket;
    }

    public String getDestinationBucket() {return destinationBucket;}

    public void setDestinationBucket(String destinationBucket) {
        this.destinationBucket = destinationBucket;
    }

    public String getVersion() {return version;}

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return "InventoryManifest{" + "\n" +
                "sourceBucket: " + sourceBucket + ", \n" +
                "destinationBucket: " + destinationBucket + ", \n" +
                "version: " + version + ", \n" +
                "fileFormat: " + fileFormat + ", \n" +
                "fileSchema: " + fileSchema + ", \n" +
                "locators: " + locators.toString() + "\n" +
                '}';
    }

    public static class Locator implements Serializable {
        /**
         * The name of the bucket that the inventory is for.
         */
        private String key;

        /**
         * Object size in bytes of the inventory.
         */
        private long size;

        /**
         * The MD5 of the content of the inventory.
         */
        private String md5checksum;

        // Getters and setters
        public String getKey() {
            return key;
        }

        public void setKey(String key){
            this.key = key;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public String getMD5checksum() {
            return md5checksum;
        }

        @JsonProperty("MD5checksum")
        public void setMD5checksum(String MD5checksum) {
            this.md5checksum = MD5checksum;
        }

        @Override
        public boolean equals(Object o) {
            return EqualsBuilder.reflectionEquals(this, o);
        }

        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }

        @Override
        public String toString() {
            return "Locator{" +
                    "key: " + key + ", \n" +
                    "size: " + size + ", \n" +
                    "md5checksum: " + md5checksum + "\n" +
                    '}';
        }
    }
}
