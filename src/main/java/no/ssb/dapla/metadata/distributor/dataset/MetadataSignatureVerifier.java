package no.ssb.dapla.metadata.distributor.dataset;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;

public class MetadataSignatureVerifier {

    final Signature signature;

    public MetadataSignatureVerifier() {
        try {
            char[] password = new char[]{'c', 'h', 'a', 'n', 'g', 'e', 'i', 't'};
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(new FileInputStream("secret/metadata-verifier_keystore.p12"), password);
            Certificate certificate = keyStore.getCertificate("dataAccessCertificate");
            PublicKey publicKey = certificate.getPublicKey();

            signature = Signature.getInstance("SHA256withRSA");
            signature.initVerify(publicKey);
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public boolean verify(byte[] data, byte[] receivedSign) {
        try {
            signature.update(data);
            return signature.verify(receivedSign);
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        }
    }

}
