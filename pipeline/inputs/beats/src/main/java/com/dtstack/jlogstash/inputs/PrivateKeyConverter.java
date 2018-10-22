package com.dtstack.jlogstash.inputs;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

/*
 * copy from https://github.com/elastic/java-lumber
 * Take an Pem RSA Private key and convert it to a Pkcs8 private key that netty
 * can understand.
 *
 */
public class PrivateKeyConverter {
    static final Logger logger = LoggerFactory.getLogger(PrivateKeyConverter.class.getName());

    private final String passphrase;
    private FileReader file;

    public PrivateKeyConverter(String filepath, String pass) throws FileNotFoundException {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        file = new FileReader(filepath);
        passphrase = pass;
    }


    public InputStream convert() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
        logger.debug("Converting Private keys if needed");
        PrivateKey kp = loadKeyPair();
        return generatePkcs8(kp);
    }

    private InputStream generatePkcs8(PrivateKey kp) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        logger.debug("Generate a Pkcs8 private key: " + kp.getFormat());


        StringWriter out = new StringWriter();
        JcaPEMWriter writer = new JcaPEMWriter(out);
        writer.writeObject(kp);
        writer.close();

        return new ByteArrayInputStream(out.toString().getBytes());
    }

    private PrivateKey loadKeyPair() throws IOException {
        PEMParser reader = new PEMParser(file);
        Object pemObject;

        JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
        //PEMDecryptorProvider decryptionProv = new JcePEMDecryptorProviderBuilder().build(passphrase);

        while((pemObject = reader.readObject()) != null) {
            logger.debug("PemObject type: " + pemObject.getClass().getName());

            if(pemObject instanceof PEMKeyPair) {
                logger.debug("it match");
                PrivateKeyInfo pki = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
                logger.debug("content: " + pki.getEncoded("UTF-8"));
                return converter.getPrivateKey(pki);
            } else {
                logger.debug("Dont match");
            }
        }

        logger.debug("fsdfsfs");
        return null;
    }

    private boolean hasPassword() {
        if(passphrase != null) {
            return true;
        } else {
            return false;
        }
    }
}