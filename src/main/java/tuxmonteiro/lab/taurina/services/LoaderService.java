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

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.codec.http2.HttpConversionUtil;
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
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@EnableAsync
@EnableScheduling
public class LoaderService {

    private final ReportService reportService;
    private final CookieService cookieService;

    enum Proto {
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
                    LOGGER.error(e.getMessage(), e);
                }
            }
            return null;
        }

        public ChannelInitializer initializer(final ReportService reportService, CookieService cookieService) {
            if (this == HTTP_2 || this == HTTPS_2) {
                return new Http2ClientInitializer(sslContext(), Integer.MAX_VALUE, reportService, cookieService);
            }
            return new Http1ClientInitializer(sslContext(), reportService, cookieService);
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

    private static final Log LOGGER = LogFactory.getLog(LoaderService.class);
    private static final boolean IS_MAC = isMac();
    private static final boolean IS_LINUX = isLinux();

    private static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public LoaderService(ReportService reportService, CookieService cookieService) {
        this.reportService = reportService;
        this.cookieService = cookieService;
    }

    private String convertSchemeIfNecessary(String scheme) {
        return scheme.replace("h2c", "https").replace("h2", "http");
    }

    private AtomicLong start = new AtomicLong(0L);

    @Async
    @Scheduled(fixedRate = 5_000L)
    public void start() {
        if (start.get() != 0) {
            return;
        } else {
            start.set(-1);
        }

        EventLoopGroup group = null;
        try {
            String jsonStr = "{\"uri\":\"http://127.0.0.1:8030\"}";

            HashMap hashMap = mapper.readValue(jsonStr, HashMap.class);

            String uriStr = (String) hashMap.get("uri");
            String methodStr = Optional.ofNullable((String) hashMap.get("method")).orElse("GET");
            HttpMethod method = HttpMethod.valueOf(methodStr);
            int numConn = Optional.ofNullable((Integer) hashMap.get("numConn")).orElse(10);
            int durationSec = Optional.ofNullable((Integer) hashMap.get("durationTimeMillis")).orElse(30000) / 1000;
            URI uriFromJson = URI.create(uriStr);
            String pathFromURI = uriFromJson.getRawPath();
            String path = pathFromURI == null || pathFromURI.isEmpty() ? "/" : pathFromURI;

            final HttpHeaders headers = new DefaultHttpHeaders()
                .add(HOST, uriFromJson.getHost() + (uriFromJson.getPort() > 0 ? ":" + uriFromJson.getPort() : ""))
                .add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), convertSchemeIfNecessary(uriFromJson.getScheme()));

            // TODO: Check cast
            @SuppressWarnings("unchecked")
            Map<String, String> headersFromJson = Optional.ofNullable((Map<String, String>) hashMap.get("headers")).orElse(Collections.emptyMap());
            headersFromJson.forEach(headers::add);

            final FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, path, Unpooled.buffer(0), headers, new DefaultHttpHeaders());
            int threads = Integer.parseInt(System.getProperty("taurina.threads",
                String.valueOf(NUM_CORES > numConn ? numConn : NUM_CORES)));

            LOGGER.info("Using " + threads + " thread(s)");

            group = getEventLoopGroup(threads);
            final Proto proto = Proto.schemaToProto(uriFromJson.getScheme());
            final Bootstrap bootstrap = newBootstrap(group);

            Channel[] channels = new Channel[numConn];
            activeChanels(numConn, proto, bootstrap, channels, uriFromJson, request);

            start.set(System.currentTimeMillis());

            reconnectIfNecessary(numConn, proto, group, bootstrap, channels, uriFromJson, request);

            TimeUnit.SECONDS.sleep(durationSec);

            reportService.showReport(start.get());
            reportService.reset();
            cookieService.reset();

            closeChannels(group, channels, 5, TimeUnit.SECONDS);

        } catch (IOException | InterruptedException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(e.getMessage(), e);
            }
        } finally {
            if (group != null && !group.isShuttingDown()) {
                group.shutdownGracefully();
            }
        }
    }

    private void reconnectIfNecessary(int numConn, final Proto proto, final EventLoopGroup group, Bootstrap bootstrap, Channel[] channels, URI uri, FullHttpRequest request) {
        group.scheduleAtFixedRate(() ->
            activeChanels(numConn, proto, bootstrap, channels, uri, request), 100, 100, TimeUnit.MICROSECONDS);
    }

    private Bootstrap newBootstrap(EventLoopGroup group) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.
            group(group).
            channel(getSocketChannelClass()).
            option(ChannelOption.SO_KEEPALIVE, true).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 60000).
            option(ChannelOption.TCP_NODELAY, true).
            option(ChannelOption.SO_REUSEADDR, true);
        return bootstrap;
    }

    private void closeChannels(EventLoopGroup group, Channel[] channels, int timeout, TimeUnit unit) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(channels.length - 1);
        for (Channel channel : channels) {
            group.execute(() -> {
                closeChannel(latch, channel);
            });
        }
        latch.await(timeout, unit);
    }



    private void closeChannel(final CountDownLatch latch, final Channel channel) {
        if (channel.isActive()) {
            try {
                channel.closeFuture().sync();
            } catch (Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(e.getMessage(), e);
                }
            } finally {
                latch.countDown();
            }
        }
    }

    private synchronized void activeChanels(int numConn, final Proto proto, final Bootstrap bootstrap, final Channel[] channels, URI uri, FullHttpRequest request) {
        for (int chanId = 0; chanId < numConn; chanId++) {
            if (channels[chanId] == null || !channels[chanId].isActive()) {
                Channel channel = newChannel(bootstrap, proto, uri, request);
                if (channel != null) {
                    channels[chanId] = channel;
                }
            }
        }
    }

    private Channel newChannel(final Bootstrap bootstrap, Proto proto, URI uri, FullHttpRequest request) {
        try {
            final Channel channel = bootstrap
                                        .clone()
                                        .handler(proto.initializer(reportService, cookieService))
                                        .connect(uri.getHost(), uri.getPort())
                                        .sync()
                                        .channel();
            channel.eventLoop().scheduleAtFixedRate(() -> {
                if (channel.isActive()) {
                    reportService.writeCounterIncr();
                    cookieService.applyCookies(request.headers());
                    channel.writeAndFlush(request.copy());
                }
            }, 50, 50, TimeUnit.MICROSECONDS);
            return channel;
        } catch (InterruptedException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(e.getMessage(), e);
            }
        }
        return null;
    }

    private EventLoopGroup getEventLoopGroup(int numCores) {
        // @formatter:off
        return IS_MAC   ? new KQueueEventLoopGroup(numCores) :
               IS_LINUX ? new EpollEventLoopGroup(numCores) :
                          new NioEventLoopGroup(numCores);
        // @formatter:on
    }

    private Class<? extends Channel> getSocketChannelClass() {
        // @formatter:off
        return IS_MAC   ? KQueueSocketChannel.class :
               IS_LINUX ? EpollSocketChannel.class :
                          NioSocketChannel.class;
        // @formatter:on
    }

    private static String getOS() {
        return System.getProperty("os.name", "UNDEF").toLowerCase();
    }

    private static boolean isMac() {
        boolean result = getOS().startsWith("mac");
        if (result) {
            LOGGER.warn("Hello. I'm Mac");
        }
        return result;
    }

    private static boolean isLinux() {
        boolean result = getOS().startsWith("linux");
        if (result) {
            LOGGER.warn("Hello. I'm Linux");
        }
        return result;
    }

}
