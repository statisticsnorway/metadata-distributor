package no.ssb.dapla.metadata.distributor.parquet;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class ParquetTools {

    public static Schema getAvroSchemaFromFile(InputFile inputFile) {
        Configuration configuration = new Configuration();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            MessageType messageType = reader.getFooter().getFileMetaData().getSchema();
            return new AvroSchemaConverter(configuration).convert(messageType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read parquet footer from " + inputFile.toString(), e);
        }
    }
}
