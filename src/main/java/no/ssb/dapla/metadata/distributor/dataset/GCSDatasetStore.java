package no.ssb.dapla.metadata.distributor.dataset;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.metadata.distributor.parquet.GCSReadChannelBasedInputFile;
import no.ssb.dapla.metadata.distributor.parquet.ParquetTools;
import org.apache.avro.Schema;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;

public class GCSDatasetStore implements DatasetStore {

    final Storage storage;
    final MetadataSignatureVerifier metadataSignatureVerifier;

    public GCSDatasetStore(Storage storage, MetadataSignatureVerifier metadataSignatureVerifier) {
        this.storage = storage;
        this.metadataSignatureVerifier = metadataSignatureVerifier;
    }

    @Override
    public MetadataReadAndVerifyResult resolveAndReadDatasetMeta(DatasetUri datasetUri) {
        byte[] datasetMetaBytes;
        byte[] datasetDocBytes = null;
        byte[] datasetLineageBytes = null;
        byte[] datasetMetaSignatureBytes;
        String datasetMetaJsonPath = datasetUri.toURI().getPath() + "/.dataset-meta.json";
        String datasetDocJsonPath = datasetUri.toURI().getPath() + "/.dataset-doc.json";
        String datasetLinageJsonPath = datasetUri.toURI().getPath() + "/.dataset-lineage.json";
        String datasetMetaJsonSignaturePath = datasetUri.toURI().getPath() + "/.dataset-meta.json.sign";
        Schema avroSchema;

        String bucket = datasetUri.toURI().getHost();
        datasetMetaBytes = storage.readAllBytes(BlobId.of(bucket, stripLeadingSlashes(datasetMetaJsonPath)));
        BlobId datasetDocBlobId = BlobId.of(bucket, stripLeadingSlashes(datasetDocJsonPath));
        Blob datasetDocBlob = storage.get(datasetDocBlobId);
        if (datasetDocBlob != null) {
            datasetDocBytes = storage.readAllBytes(datasetDocBlobId);
        }
        BlobId datasetLineageBlobId = BlobId.of(bucket, stripLeadingSlashes(datasetLinageJsonPath));
        Blob datasetLineageBlob = storage.get(datasetLineageBlobId);
        if (datasetLineageBlob != null) {
            datasetLineageBytes = storage.readAllBytes(datasetLineageBlobId);
        }

        datasetMetaSignatureBytes = storage.readAllBytes(BlobId.of(bucket, stripLeadingSlashes(datasetMetaJsonSignaturePath)));
        avroSchema = getAvroSchemaFromGoogleCloudStorage(storage, datasetUri);
        boolean verified = metadataSignatureVerifier.verify(datasetMetaBytes, datasetMetaSignatureBytes);
        return new MetadataReadAndVerifyResult(verified,
                ByteString.copyFrom(datasetMetaBytes),
                ofNullable(datasetDocBytes).map(ByteString::copyFrom).orElse(null),
                ofNullable(datasetLineageBytes).map(ByteString::copyFrom).orElse(null),
                avroSchema
        );
    }

    @Override
    public String supportedScheme() {
        return "gs";
    }

    private static Schema getAvroSchemaFromGoogleCloudStorage(Storage storage, DatasetUri datasetUri) {
        String bucket = datasetUri.toURI().getHost();
        String prefix = datasetUri.toURI().getPath();
        Page<Blob> page = storage.list(bucket, Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.pageSize(10));
        Blob firstParquetBlob = paginateUntil(page, b -> b.getName().endsWith(".parquet"));
        if (firstParquetBlob == null) {
            return null;
        }
        Schema schema = ParquetTools.getAvroSchemaFromFile(new GCSReadChannelBasedInputFile(firstParquetBlob));
        return schema;
    }

    private static Blob paginateUntil(Page<Blob> firstPage, Predicate<? super Blob> predicate) {
        return StreamSupport.stream(firstPage.iterateAll().spliterator(), false)
                .filter(predicate)
                .findFirst()
                .orElseGet(() -> {
                    Page<Blob> page = firstPage;
                    while (page.hasNextPage()) {
                        page = page.getNextPage();
                        Optional<Blob> first = StreamSupport.stream(page.iterateAll().spliterator(), false)
                                .filter(predicate)
                                .findFirst();
                        if (first.isPresent()) {
                            return first.get();
                        }
                    }
                    return null;
                });
    }

    private static String stripLeadingSlashes(String input) {
        return input.startsWith("/") ? stripLeadingSlashes(input.substring(1)) : input;
    }
}
