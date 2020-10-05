package no.ssb.dapla.metadata.distributor.dataset;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class ParquetTools {

    public static Schema getAvroSchemaFromFile(String parquetFilePath) {
        Configuration configuration = new Configuration();
        Path parquetPath = new Path(parquetFilePath);

        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, configuration))) {
            MessageType messageType = reader.getFooter().getFileMetaData().getSchema();
            return new AvroSchemaConverter(configuration).convert(messageType);

        } catch (IOException e) {
            throw new RuntimeException("Failed to read parquet header from " + parquetPath, e);
        }
    }
}
