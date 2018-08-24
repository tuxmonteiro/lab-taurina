package tuxmonteiro.lab.taurina.enumerator;

import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import javax.net.ssl.SSLException;
import tuxmonteiro.lab.taurina.nettyutils.Http1ClientInitializer;
import tuxmonteiro.lab.taurina.nettyutils.Http2ClientInitializer;
import tuxmonteiro.lab.taurina.entity.ReportService;

public enum Proto {

        HTTPS_1(true),
        HTTPS_2(true),
        HTTP_1(false),
        HTTP_2(false);

        private final boolean ssl;

        Proto(boolean ssl) {
            this.ssl = ssl;
        }

        public SslContext sslContext() {
            if (ssl) {
                try {
                    final SslProvider provider = OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK;
                    return SslContextBuilder.forClient()
                        .sslProvider(provider)
                        /* NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification.
                         * Please refer to the HTTP/2 specification for cipher requirements. */
                        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .applicationProtocolConfig(new ApplicationProtocolConfig(
                            Protocol.ALPN,
                            // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
                            SelectorFailureBehavior.NO_ADVERTISE,
                            // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
                            SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2,
                            ApplicationProtocolNames.HTTP_1_1))
                        .build();
                } catch (SSLException e) {
                  //  LOGGER.error(e.getMessage(), e);
                }
            }
            return null;
        }

        public ChannelInitializer initializer(final ReportService reportService) {
            if (this == HTTP_2 || this == HTTPS_2) {
                return new Http2ClientInitializer(sslContext(), Integer.MAX_VALUE, reportService);
            }
            return new Http1ClientInitializer(sslContext(), reportService);
        }

        public static Proto schemaToProto(String schema) {
            switch (schema) {
                case "h2":
                    return HTTP_2;
                case "h2c":
                    return HTTPS_2;
                case "http":
                    return HTTP_1;
                case "https":
                    return HTTPS_1;
            }
            return Proto.valueOf(schema);
        }
    }


