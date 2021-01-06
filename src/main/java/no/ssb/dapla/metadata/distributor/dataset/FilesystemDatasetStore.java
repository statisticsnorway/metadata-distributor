package no.ssb.dapla.metadata.distributor.dataset;

import com.google.protobuf.ByteString;
import io.helidon.metrics.RegistryFactory;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.metadata.distributor.parquet.NIOPathBasedInputFile;
import no.ssb.dapla.metadata.distributor.parquet.ParquetTools;
import org.apache.avro.Schema;
import org.eclipse.microprofile.metrics.Meter;
import org.eclipse.microprofile.metrics.MetricRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Optional.ofNullable;

public class FilesystemDatasetStore implements DatasetStore {

    final String fileSystemDataFolder;
    final MetadataSignatureVerifier metadataSignatureVerifier;

    private final Meter fsCheckFileMeter;
    private final Meter fsReadFileMeter;
    private final Meter fsListMeter;
    private final Meter fsReadParquetSchemaMeter;

    public FilesystemDatasetStore(String fileSystemDataFolder, MetadataSignatureVerifier metadataSignatureVerifier) {
        this.fileSystemDataFolder = fileSystemDataFolder;
        this.metadataSignatureVerifier = metadataSignatureVerifier;
        RegistryFactory metricsRegistry = RegistryFactory.getInstance();
        MetricRegistry appRegistry = metricsRegistry.getRegistry(MetricRegistry.Type.APPLICATION);
        this.fsCheckFileMeter = appRegistry.meter("fsCheckFileMeter");
        this.fsReadFileMeter = appRegistry.meter("fsReadFileMeter");
        this.fsListMeter = appRegistry.meter("fsListMeter");
        this.fsReadParquetSchemaMeter = appRegistry.meter("fsReadParquetSchemaMeter");
    }

    @Override
    public MetadataReadAndVerifyResult resolveAndReadDatasetMeta(DatasetUri datasetUri) {
        try {
            byte[] datasetMetaBytes;
            byte[] datasetDocBytes = null;
            byte[] datasetLineageBytes = null;
            byte[] datasetMetaSignatureBytes;
            String datasetMetaJsonPath = datasetUri.toURI().getPath() + "/.dataset-meta.json";
            String datasetDocJsonPath = datasetUri.toURI().getPath() + "/.dataset-doc.json";
            String datasetLinageJsonPath = datasetUri.toURI().getPath() + "/.dataset-lineage.json";
            String datasetMetaJsonSignaturePath = datasetUri.toURI().getPath() + "/.dataset-meta.json.sign";
            Schema avroSchema;
            datasetMetaBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetMetaJsonPath));
            fsReadFileMeter.mark();
            if (Files.isReadable(Path.of(fileSystemDataFolder, datasetDocJsonPath))) {
                datasetDocBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetDocJsonPath));
                fsReadFileMeter.mark();
            }
            fsCheckFileMeter.mark();
            if (Files.isReadable(Path.of(fileSystemDataFolder, datasetLinageJsonPath))) {
                datasetLineageBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetLinageJsonPath));
                fsReadFileMeter.mark();
            }
            fsCheckFileMeter.mark();
            datasetMetaSignatureBytes = Files.readAllBytes(Path.of(fileSystemDataFolder, datasetMetaJsonSignaturePath));
            fsReadFileMeter.mark();
            avroSchema = getAvroSchemaFromLocalFileSystem(datasetUri);
            boolean verified = metadataSignatureVerifier.verify(datasetMetaBytes, datasetMetaSignatureBytes);
            return new MetadataReadAndVerifyResult(verified,
                    ByteString.copyFrom(datasetMetaBytes),
                    ofNullable(datasetDocBytes).map(ByteString::copyFrom).orElse(null),
                    ofNullable(datasetLineageBytes).map(ByteString::copyFrom).orElse(null),
                    avroSchema
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String supportedScheme() {
        return "file";
    }

    private Schema getAvroSchemaFromLocalFileSystem(DatasetUri datasetUri) {
        Path firstParquetFile;
        try {
            String datasetFolder = datasetUri.toURI().getRawPath();
            fsListMeter.mark();
            firstParquetFile = Files.list(Path.of(datasetFolder))
                    .filter(p -> p.getFileName().toString().endsWith(".parquet"))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No parquet file in folder: " + datasetFolder));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Schema schema = ParquetTools.getAvroSchemaFromFile(new NIOPathBasedInputFile(firstParquetFile));
        fsReadParquetSchemaMeter.mark();
        return schema;
    }
}
