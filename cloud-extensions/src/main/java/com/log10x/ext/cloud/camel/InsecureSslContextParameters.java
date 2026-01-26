package com.log10x.ext.cloud.camel;

import org.apache.camel.support.jsse.SSLContextParameters;
import org.apache.camel.support.jsse.TrustManagersParameters;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

/**
 * An SSLContextParameters implementation that trusts all certificates.
 *
 * WARNING: This should only be used for development/testing purposes,
 * not in production environments.
 */
public class InsecureSslContextParameters extends SSLContextParameters {

    public InsecureSslContextParameters() {
        TrustManagersParameters trustManagersParameters = new TrustManagersParameters() {
            @Override
            public TrustManager[] createTrustManagers() {
                return new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {
                            // Trust all clients
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] chain, String authType) {
                            // Trust all servers
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }
                };
            }
        };
        setTrustManagers(trustManagersParameters);
    }
}
