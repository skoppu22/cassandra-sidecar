/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.config.yaml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.http.ClientAuth;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;

/**
 * Encapsulates SSL Configuration
 */
public class SslConfigurationImpl implements SslConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SslConfigurationImpl.class);
    public static final boolean DEFAULT_SSL_ENABLED = false;
    public static final boolean DEFAULT_USE_OPEN_SSL = true;
    public static final SecondBoundConfiguration DEFAULT_HANDSHAKE_TIMEOUT = SecondBoundConfiguration.parse("10s");
    public static final String DEFAULT_CLIENT_AUTH = "NONE";
    public static final List<String> DEFAULT_SECURE_TRANSPORT_PROTOCOLS
    = Collections.unmodifiableList(Arrays.asList("TLSv1.2", "TLSv1.3"));


    @JsonProperty("enabled")
    protected final boolean enabled;

    @JsonProperty(value = "use_openssl")
    protected final boolean useOpenSsl;

    protected SecondBoundConfiguration handshakeTimeout;

    protected String clientAuth;

    @JsonProperty(value = "cipher_suites")
    protected final List<String> cipherSuites;

    @JsonProperty(value = "accepted_protocols")
    protected final List<String> secureTransportProtocols;

    @JsonProperty("keystore")
    protected final KeyStoreConfiguration keystore;

    @JsonProperty("truststore")
    protected final KeyStoreConfiguration truststore;

    public SslConfigurationImpl()
    {
        this(builder());
    }

    protected SslConfigurationImpl(Builder builder)
    {
        enabled = builder.enabled;
        useOpenSsl = builder.useOpenSsl;
        handshakeTimeout = builder.handshakeTimeout;
        setClientAuth(builder.clientAuth);
        keystore = builder.keystore;
        truststore = builder.truststore;
        cipherSuites = builder.cipherSuites;
        secureTransportProtocols = builder.secureTransportProtocols;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("enabled")
    public boolean enabled()
    {
        return enabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "use_openssl")
    public boolean preferOpenSSL()
    {
        return useOpenSsl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "handshake_timeout")
    public SecondBoundConfiguration handshakeTimeout()
    {
        return handshakeTimeout;
    }

    @JsonProperty(value = "handshake_timeout")
    public void setHandshakeTimeout(SecondBoundConfiguration handshakeTimeout)
    {
        this.handshakeTimeout = handshakeTimeout;
    }

    /**
     * Legacy property {@code handshake_timeout_sec}
     *
     * @param handshakeTimeoutInSeconds timeout in seconds
     * @deprecated in favor of {@code handshake_timeout}
     */
    @JsonProperty(value = "handshake_timeout_sec")
    @Deprecated
    public void setHandshakeTimeoutInSeconds(long handshakeTimeoutInSeconds)
    {
        LOGGER.warn("'handshake_timeout_sec' is deprecated, use 'handshake_timeout' instead");
        setHandshakeTimeout(new SecondBoundConfiguration(handshakeTimeoutInSeconds, TimeUnit.SECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "client_auth")
    public String clientAuth()
    {
        return clientAuth;
    }

    @JsonProperty(value = "client_auth")
    public void setClientAuth(String clientAuth)
    {
        this.clientAuth = clientAuth;
        try
        {
            // forces a validation of the input
            this.clientAuth = ClientAuth.valueOf(clientAuth).name();
        }
        catch (IllegalArgumentException exception)
        {
            String errorMessage = String.format("Invalid client_auth configuration=\"%s\", valid values are (%s)",
                                                clientAuth,
                                                Arrays.stream(ClientAuth.values())
                                                      .map(Enum::name)
                                                      .collect(Collectors.joining(",")));
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "cipher_suites")
    public List<String> cipherSuites()
    {
        return cipherSuites;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "accepted_protocols")
    public List<String> secureTransportProtocols()
    {
        return secureTransportProtocols;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("keystore")
    public KeyStoreConfiguration keystore()
    {
        return keystore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTrustStoreConfigured()
    {
        return truststore != null && truststore.isConfigured();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("truststore")
    public KeyStoreConfiguration truststore()
    {
        return truststore;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code SslConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, SslConfigurationImpl>
    {
        protected boolean enabled = DEFAULT_SSL_ENABLED;
        protected boolean useOpenSsl = DEFAULT_USE_OPEN_SSL;
        protected SecondBoundConfiguration handshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT;
        protected String clientAuth = DEFAULT_CLIENT_AUTH;
        protected List<String> cipherSuites = Collections.emptyList();
        protected List<String> secureTransportProtocols = DEFAULT_SECURE_TRANSPORT_PROTOCOLS;
        protected KeyStoreConfiguration keystore = null;
        protected KeyStoreConfiguration truststore = null;

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code enabled} and returns a reference to this Builder enabling method chaining.
         *
         * @param enabled the {@code enabled} to set
         * @return a reference to this Builder
         */
        public Builder enabled(boolean enabled)
        {
            return update(b -> b.enabled = enabled);
        }

        /**
         * Sets the {@code useOpenSsl} and returns a reference to this Builder enabling method chaining.
         *
         * @param useOpenSsl the {@code useOpenSsl} to set
         * @return a reference to this Builder
         */
        public Builder useOpenSsl(boolean useOpenSsl)
        {
            return update(b -> b.useOpenSsl = useOpenSsl);
        }

        /**
         * Sets the {@code handshakeTimeout} and returns a reference to this Builder enabling method chaining.
         *
         * @param handshakeTimeout the {@code handshakeTimeout} to set
         * @return a reference to this Builder
         */
        public Builder handshakeTimeout(SecondBoundConfiguration handshakeTimeout)
        {
            return update(b -> b.handshakeTimeout = handshakeTimeout);
        }

        /**
         * Sets the {@code clientAuth} and returns a reference to this Builder enabling method chaining.
         *
         * @param clientAuth the {@code clientAuth} to set
         * @return a reference to this Builder
         */
        public Builder clientAuth(String clientAuth)
        {
            return update(b -> b.clientAuth = clientAuth);
        }

        /**
         * Sets the {@code cipherSuites} and returns a reference to this Builder enabling method chaining.
         *
         * @param cipherSuites the {@code cipherSuites} to set
         * @return a reference to this Builder
         */
        public Builder cipherSuites(List<String> cipherSuites)
        {
            return update(b -> b.cipherSuites = new ArrayList<>(cipherSuites));
        }

        /**
         * Sets the {@code secureTransportProtocols} and returns a reference to this Builder enabling method chaining.
         *
         * @param secureTransportProtocols the {@code secureTransportProtocols} to set
         * @return a reference to this Builder
         */
        public Builder secureTransportProtocols(List<String> secureTransportProtocols)
        {
            return update(b -> b.secureTransportProtocols = new ArrayList<>(secureTransportProtocols));
        }

        /**
         * Sets the {@code keystore} and returns a reference to this Builder enabling method chaining.
         *
         * @param keystore the {@code keystore} to set
         * @return a reference to this Builder
         */
        public Builder keystore(KeyStoreConfiguration keystore)
        {
            return update(b -> b.keystore = keystore);
        }

        /**
         * Sets the {@code truststore} and returns a reference to this Builder enabling method chaining.
         *
         * @param truststore the {@code truststore} to set
         * @return a reference to this Builder
         */
        public Builder truststore(KeyStoreConfiguration truststore)
        {
            return update(b -> b.truststore = truststore);
        }

        /**
         * Returns a {@code SslConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code SslConfigurationImpl} built with parameters of this {@code SslConfigurationImpl.Builder}
         */
        @Override
        public SslConfigurationImpl build()
        {
            return new SslConfigurationImpl(this);
        }
    }
}
