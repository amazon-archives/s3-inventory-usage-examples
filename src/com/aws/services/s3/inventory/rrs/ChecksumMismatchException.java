/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

/**
 * The ChecksumMismatchException is thrown when a calculated Checksum doesn't match the expected value.
 */
public class ChecksumMismatchException extends RuntimeException{
    /**
     * Constructs a new ChecksumMismatchException instance.
     * @param expectedChecksum that was not received.
     * @param actualChecksum that was calculated.
     */
    public ChecksumMismatchException(String expectedChecksum, String actualChecksum) {
        super("A Checksum mismatch occurred [Expected: " + expectedChecksum + "] [Actual: " + actualChecksum + "]");
    }
}
