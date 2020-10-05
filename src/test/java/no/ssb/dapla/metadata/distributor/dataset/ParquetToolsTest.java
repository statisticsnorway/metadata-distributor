package no.ssb.dapla.metadata.distributor.dataset;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;

class ParquetToolsTest {

    @Test
    void checkAvroSchemaFromFile() throws URISyntaxException {
        URL resourceUrl = getClass().getResource("dataset.parquet");
        Schema avroSchemaFromFile = ParquetTools.getAvroSchemaFromFile(resourceUrl.toURI().getRawPath());
        System.out.println(avroSchemaFromFile);
    }
}