/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * This InventoryReportLine class provides a way to marshalling the inventory report in CSV format.
 * It stores all the elements of the inventory report and provides a constructor to map them into POJOs
 */
public class InventoryReportLine implements Serializable {

    /**
     * The name of the bucket that the inventory is for.
     * Required field included in inventory report.
     */
    @JsonProperty("Bucket")
    private String bucket;

    /**
     * Object key name (or key) that uniquely identifies the object in the bucket.
     * Required field included in inventory report.
     */
    @JsonProperty("Key")
    private String key;

    /**
     * Object version ID.
     * This field is not included if the list is only for the current version of objects.
     */
    @JsonProperty("Versionid")
    private String versionId;

    /**
     * Show if the object is the current version of the object.
     * This field is not included if the list is only for the current version of objects.
     */
    @JsonProperty("IsLatest")
    private String isLatest;

    /**
     * Show if the object is a delete marker.
     * This field is not included if the list is only for the current version of objects.
     */
    @JsonProperty("IsDeleteMaker")
    private String isDeleteMaker;

    /**
     * Object size in bytes.
     * Optional field included in inventory report.
     */
    @JsonProperty("Size")
    private String size;

    /**
     * Object creation date or the last modified date, whichever is the latest.
     * Optional field included in inventory report.
     */
    @JsonProperty("LastModifiedDate")
    private String lastModifiedDate;

    /**
     * The entity tag is a hash of the object.
     * Optional field included in inventory report.
     */
    @JsonProperty("ETag")
    private String eTag;

    /**
     * Storage class used for storing the object.
     * Optional field included in inventory report.
     */
    @JsonProperty("StorageClass")
    private String storageClass;

    /**
     * Show if the object was uploaded as a multipart upload.
     * Optional field included in inventory report.
     */
    @JsonProperty("IsMultipartUploaded")
    private String multiPartUploaded;

    /**
     * Show the object replication status is PENDING, COMPLETED, FAILED, or REPLICA.
     * Optional field included in inventory report.
     */
    @JsonProperty("ReplicationStatus")
    private String replicationStatus;

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return "InventoryReportLine{" +
                "bucket='" + bucket + '\'' +
                ", key='" + key + '\'' +
                ", versionId='" + versionId + '\'' +
                ", isLatest='" + isLatest + '\'' +
                ", isDeleteMaker='" + isDeleteMaker + '\'' +
                ", size='" + size + '\'' +
                ", lastModifiedDate='" + lastModifiedDate + '\'' +
                ", eTag='" + eTag + '\'' +
                ", storageClass='" + storageClass + '\'' +
                ", multiPartUploaded='" + multiPartUploaded + '\'' +
                ", replicationStatus='" + replicationStatus + '\'' +
                '}';
    }

    // Getters and setters
    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getIsLatest() {
        return isLatest;
    }

    public void setIsLatest(String isLatest) {
        this.isLatest = isLatest;
    }

    public String getIsDeleteMaker() {
        return isDeleteMaker;
    }

    public void setIsDeleteMaker(String isDeleteMaker) {
        this.isDeleteMaker = isDeleteMaker;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(String lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    public String getEtag() {
        return eTag;
    }

    public void seteTag(String eTag) {
        this.eTag = eTag;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public String getMultiPartUploaded() {
        return multiPartUploaded;
    }

    public void setMultiPartUploaded(String multiPartUploaded) {
        this.multiPartUploaded = multiPartUploaded;
    }

    public String getReplicationStatus() {
        return replicationStatus;
    }

    public void setReplicationStatus(String replicationStatus) {
        this.replicationStatus = replicationStatus;
    }
}
