/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * This ArgumentParser class parses the arguments, and returns a "BucketKey"
 * which represents the bucket names and keys of the source bucket and the output bucket.
 */
class ArgumentParser {
    private static final String KEY_INPUT = "i";
    private static final String KEY_OUTPUT = "o";
    private static final String longOptInput = "srcS3Uri";
    private static final String longOptOutput = "outputS3UriPrefix";
    private final CommandLineParser parser;
    private Options options;

    public ArgumentParser() {
        final Option input =
                new Option(KEY_INPUT, longOptInput, true,
                        "read in this S3 URI to get inventory manifest files");
        final Option output =
                new Option(KEY_OUTPUT, longOptOutput, true,
                        "write new files back to this output S3 URI prefix");
        this.options = new Options();
        input.setRequired(true);
        this.options.addOption(input);
        output.setRequired(true);
        this.options.addOption(output);
        this.parser = new PosixParser();
    }

    /**
     * Parse the argument and extract the source and output locations.
     * @param args Arguments in the command line
     * @return BucketKey, which is an object containing the buckets and the keys.
     */
    public BucketKey parseArguments(String[] args) throws ParseException {
        final CommandLine cmd = parser.parse(options, args);
        String inputFilePath = cmd.getOptionValue(KEY_INPUT);
        String outputFilePath = cmd.getOptionValue(KEY_OUTPUT);
        return new BucketKey(inputFilePath, outputFilePath);
    }
}
