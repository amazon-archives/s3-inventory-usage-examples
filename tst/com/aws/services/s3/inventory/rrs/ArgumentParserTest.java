/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test on parsing argument
 */
public class ArgumentParserTest {
    private static ArgumentParser argumentParser;

    @Before
    public void setUp(){
        argumentParser = new ArgumentParser();
    }

    @Test
    public void parseArgumentSuccess() throws Exception {
        String[] args = {"-i", "s3://<srcBucketTest>/<srcKeyTest>", "-o", "s3://<destBucketTest>/<destPathTest>"};
        BucketKey result = argumentParser.parseArguments(args);
        assertThat(result.getSrcBucket(), is("<srcBucketTest>"));
        assertThat(result.getSrcKey(), is("<srcKeyTest>"));
        assertThat(result.getDestBucket(), is("<destBucketTest>"));
        assertThat(result.getDestPrefix(), is("<destPathTest>"));
    }

    @Test (expected = ParseException.class)
    public void parseArgumentUnrecognizedOption() throws Exception{
        String[] args = {"-i", "testInput", "-x", "testOutput"};
        argumentParser.parseArguments(args);
    }

    @Test (expected = ParseException.class)
    public void parseArgumentMissingOutputOption() throws Exception{
        String[] args = {"-i", "testInput"};
        argumentParser.parseArguments(args);
    }

    @Test (expected = ParseException.class)
    public void parseArgumentMissingInputOption() throws Exception{
        String[] args = {"-o", "testOutput"};
        argumentParser.parseArguments(args);
    }

    @Test (expected = ParseException.class)
    public void parseArgumentMissingBothOptions() throws Exception{
        String[] args = {};
        argumentParser.parseArguments(args);
    }
}