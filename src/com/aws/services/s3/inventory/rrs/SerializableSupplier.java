/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;


import java.io.Serializable;
import java.util.function.Supplier;

/**
 * This SerializableSupplier class provides an interface to make a serializable wrapper
 */
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
