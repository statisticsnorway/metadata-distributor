package no.ssb.dapla.metadata.distributor.dataset;

public interface MetadataSignatureVerifier {

    boolean verify(byte[] data, byte[] receivedSign);
}
