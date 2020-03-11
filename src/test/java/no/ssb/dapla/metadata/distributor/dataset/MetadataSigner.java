package no.ssb.dapla.metadata.distributor.dataset;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;

public class MetadataSigner {

    final Signature signer;
    final Signature verifier;

    public MetadataSigner(String keystoreFormat, String keystorePath, String keyAlias, char[] password, String algorithm) {
        try {
            KeyStore keyStore = KeyStore.getInstance(keystoreFormat);
            keyStore.load(new FileInputStream(keystorePath), password);
            PrivateKey privateKey = (PrivateKey) keyStore.getKey(keyAlias, password);
            Certificate certificate = keyStore.getCertificate(keyAlias);
            PublicKey publicKey = certificate.getPublicKey();

            signer = Signature.getInstance(algorithm);
            signer.initSign(privateKey);
            verifier = Signature.getInstance(algorithm);
            verifier.initVerify(publicKey);
        } catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | InvalidKeyException | UnrecoverableKeyException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] sign(byte[] data) {
        try {
            signer.update(data);
            return signer.sign();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean verify(byte[] data, byte[] receivedSign) {
        try {
            verifier.update(data);
            return verifier.verify(receivedSign);
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        }
    }
}
