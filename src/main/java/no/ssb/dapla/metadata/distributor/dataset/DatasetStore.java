package no.ssb.dapla.metadata.distributor.dataset;

import no.ssb.dapla.dataset.uri.DatasetUri;

public interface DatasetStore {
    MetadataReadAndVerifyResult resolveAndReadDatasetMeta(DatasetUri datasetUri);

    String supportedScheme();
}
