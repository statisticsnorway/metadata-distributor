package no.ssb.dapla.metadata.distributor.dataset;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParquetToolsTest {

    @Test
    void checkAvroSchemaFromFile() {

        Schema avroSchemaFromFile = ParquetTools.getAvroSchemaFromFile("file:///../localstack/bin/testdata/skatt/person/rawdata-2019/1582719098762/skatt-v0.53.parquet");
    }
}