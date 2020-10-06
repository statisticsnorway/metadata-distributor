package no.ssb.dapla.metadata.distributor.dataset;

import com.google.protobuf.ByteString;
import org.apache.avro.Schema;

class MetadataReadAndVerifyResult {

    final boolean signatureValid;
    final ByteString datasetMetaByteString;
    final ByteString datasetDocByteString;
    final ByteString datasetLineageByteString;
    final Schema schema;

    MetadataReadAndVerifyResult(boolean signatureValid,
                                ByteString datasetMetaByteString,
                                ByteString datasetDocByteString,
                                ByteString datasetLineageByteString,
                                Schema schema) {
        this.signatureValid = signatureValid;
        this.datasetMetaByteString = datasetMetaByteString;
        this.datasetDocByteString = datasetDocByteString;
        this.datasetLineageByteString = datasetLineageByteString;
        this.schema = schema;
    }
}
