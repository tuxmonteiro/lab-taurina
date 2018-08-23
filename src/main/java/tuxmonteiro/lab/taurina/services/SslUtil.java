/*
 * Copyright (c) 2017-2018 Globo.com
 * All rights reserved.
 *
 * This source is subject to the Apache License, Version 2.0.
 * Please see the LICENSE file for more information.
 *
 * Authors: See AUTHORS file
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tuxmonteiro.lab.taurina.services;

import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.net.ssl.SSLException;

import java.security.Provider;
import java.security.Security;
import java.util.stream.Stream;

import static java.security.Security.addProvider;
import static java.security.Security.getProvider;

public class SslUtil {

    private static final Log LOGGER = LogFactory.getLog(SslUtil.class);

    public SslContext context(String provider) {
            try {
                final SslContextBuilder sslContextBuilder;
                if (provider == null || "bc".equals(provider)) {
                    addProvider(new BouncyCastleProvider());
                    final Provider sslContextProvider = getProvider("BC");
                    sslContextBuilder = SslContextBuilder.forClient().sslContextProvider(sslContextProvider);
                    LOGGER.info("SslProvider: Using " + sslContextProvider);
                } else {
                    SslProvider sslProvider = OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK;
                    sslContextBuilder = SslContextBuilder.forClient().sslProvider(sslProvider);
                    LOGGER.info("SslProvider: Using " + sslProvider);
                }
                final SslContext sslContext = sslContextBuilder
                        /* NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification.
                         * Please refer to the HTTP/2 specification for cipher requirements. */
                        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .applicationProtocolConfig(new ApplicationProtocolConfig(
                                ApplicationProtocolConfig.Protocol.ALPN,
                                // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
                                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                                // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
                                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                                ApplicationProtocolNames.HTTP_2,
                                ApplicationProtocolNames.HTTP_1_1))
                        .build();
                return sslContext;
        } catch (SSLException e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }
}
