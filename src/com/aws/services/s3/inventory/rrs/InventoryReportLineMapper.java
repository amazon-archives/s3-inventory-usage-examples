/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.aws.services.s3.inventory.rrs;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This InventoryReportLineMapper class maps the inventory report into InventoryReportLine POJOs
 */
public class InventoryReportLineMapper implements Serializable {
    private CsvSchema schema;


    public InventoryReportLineMapper(InventoryManifest inventoryManifest)
            throws Exception{
        this.schema = CsvSchemaFactory.buildSchema(inventoryManifest);
    }

    /**
     * Map each line of the inventory report into a POJO
     * @return List<InventoryReportLine> which is a list of POJOs
     * @throws IOException when mapping with schema fails
     */
    public List<InventoryReportLine> mapInventoryReportLine(List<String> inventoryReportLine) throws IOException{
        CsvMapper mapper = new CsvMapper();
        List<InventoryReportLine> inventoryReportLines = new ArrayList();

        for (String eachLine : inventoryReportLine) {
            MappingIterator<InventoryReportLine> iterator =
                    mapper.readerFor(InventoryReportLine.class).with(schema).readValues(eachLine);
            List<InventoryReportLine> rowValue = iterator.readAll();
            inventoryReportLines.add(rowValue.get(0));
        }
        return inventoryReportLines;
    }
}
