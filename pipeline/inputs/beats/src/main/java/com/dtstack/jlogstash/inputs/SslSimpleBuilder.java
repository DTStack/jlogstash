package com.dtstack.jlogstash.inputs;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * copy from https://github.com/elastic/java-lumber
 *
 */
public class SslSimpleBuilder {
    public static Logger logger = LoggerFactory.getLogger(SslSimpleBuilder.class.getName());
    private InputStream sslKeyFile;
    private InputStream sslCertificateFile;

    /*
    Mordern Ciphers List from
    https://wiki.mozilla.org/Security/Server_Side_TLS
    */
    public final static String[] DEFAULT_CIPHERS = new String[] {
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA38",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
    };

    private String[] ciphers;
    private String[] protocols = new String[] { "TLSv1.2" };
    private String certificateAuthorities;
    private String verifyMode;
    private String passPhrase;

    public SslSimpleBuilder(String sslCertificateFilePath, String sslKeyFilePath, String pass) throws FileNotFoundException {
        sslCertificateFile = createFileInputStream(sslCertificateFilePath);
        sslKeyFile = createFileInputStream(sslKeyFilePath);
        passPhrase = pass;
        ciphers = DEFAULT_CIPHERS;
    }

    public SslSimpleBuilder(InputStream sslCertificateFilePath, InputStream sslKeyFilePath, String pass) throws FileNotFoundException {
        sslCertificateFile = sslCertificateFilePath;
        sslKeyFile = sslKeyFilePath;
        passPhrase = pass;
    }

    public SslSimpleBuilder setProtocols(String[] protocols) {
        protocols = protocols;
        return this;
    }

    public SslSimpleBuilder setCipherSuites(String [] ciphersSuite) {
        ciphers = ciphersSuite;
        return this;
    }

    public SslSimpleBuilder setCertificateAuthorities(String cert) {
        certificateAuthorities = cert;
        return this;
    }

    public SslSimpleBuilder setVerifyMode(String mode) {
        verifyMode = mode;
        return this;
    }

    public InputStream getSslKeyFile() {
        return sslKeyFile;
    }

    public InputStream getSslCertificateFile() {
        return sslCertificateFile;
    }

    public SslHandler build(ByteBufAllocator bufferAllocator) throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forServer(sslCertificateFile, sslKeyFile, passPhrase);

        builder.ciphers(Arrays.asList(ciphers));

        if(requireClientAuth()) {
            logger.debug("Certificate Authorities: " + certificateAuthorities);
            builder.trustManager(new File(certificateAuthorities));
        }

        SslContext context = builder.build();
        SslHandler sslHandler = context.newHandler(bufferAllocator);

        SSLEngine engine = sslHandler.engine();
        engine.setEnabledProtocols(protocols);


        if(requireClientAuth()) {
            engine.setUseClientMode(false);
            engine.setNeedClientAuth(true);
        }

        return sslHandler;
    }

    private boolean requireClientAuth() {
        if(certificateAuthorities != null) {
            return true;
        }

        return false;
    }

    private FileInputStream createFileInputStream(String filepath) throws FileNotFoundException {
        return new FileInputStream(filepath);
    }
}