package no.ssb.dapla.metadata.distributor.parquet;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

class ParquetToolsTest {

    @Test
    void checkAvroSchemaFromClasspathResource() throws URISyntaxException, IOException {
        URL resourceUrl = getClass().getResource("dataset.parquet");
        Path parquetPath = new Path(resourceUrl.toURI().getRawPath());
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(parquetPath, new Configuration());
        Schema avroSchemaFromFile = ParquetTools.getAvroSchemaFromFile(hadoopInputFile);
        System.out.println(avroSchemaFromFile);
    }

    @Test
    void checkAvroSchemaFromLocalFilesystem() throws IOException {
        String currentWorkingDirectory = System.getProperty("user.dir");
        String absoluteFilePath = currentWorkingDirectory + "/src/test/resources/no/ssb/dapla/metadata/distributor/parquet/dataset.parquet";
        Path parquetPath = new Path("file", null, absoluteFilePath);
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(parquetPath, new Configuration());
        Schema avroSchemaFromFile = ParquetTools.getAvroSchemaFromFile(hadoopInputFile);
        System.out.println(avroSchemaFromFile);
    }
}