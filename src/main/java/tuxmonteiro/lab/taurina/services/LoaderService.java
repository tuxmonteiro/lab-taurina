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
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    enum Proto {
        HTTPS_1(true),
        HTTPS_2(true),
        HTTP_1(false),
        HTTP_2(false);

        private final boolean ssl;
        private final AtomicBoolean finished;

        Proto(boolean ssl) {
            this.finished = new AtomicBoolean(false);
            this.ssl = ssl;
        }

        public boolean isSsl() {
            return ssl;
        }

        public AtomicBoolean finished() {
            return finished;
        }

        public SslContext sslContext() {
            if (isSsl()) {
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

        public ChannelInitializer initializer(final ReportService reportService) {
            if (this == HTTP_2 || this == HTTPS_2) {
                return new Http2ClientInitializer(sslContext(), Integer.MAX_VALUE, reportService);
            }
            return new Http1ClientInitializer(sslContext(), finished, reportService);
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
            return null;
        }
    }

    private static final Log LOGGER = LogFactory.getLog(LoaderService.class);
    private static final boolean IS_MAC = isMac();
    private static final boolean IS_LINUX = isLinux();

    private static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

    private final int numConn = Integer.parseInt(System.getProperty("taurina.numconn", "10"));
    private final int durationSec = Integer.parseInt(System.getProperty("taurina.duration", "30"));
    private final HttpMethod method = HttpMethod.GET;
    private final String uriStr = System.getProperty("taurina.uri", "h2c://127.0.0.1:8445");
    private final URI uri = URI.create(uriStr);
    private final String path = System.getProperty("taurina.targetpath", "/");
    private final int threads = Integer.parseInt(System.getProperty("taurina.threads",
        String.valueOf(NUM_CORES > numConn ? numConn : NUM_CORES)));

    private final HttpHeaders headers = new DefaultHttpHeaders()
        .add(HOST, uri.getHost() + (uri.getPort() > 0 ? ":" + uri.getPort() : ""))
        .add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), convertSchemeIfNecessary(uri.getScheme()));

    @Autowired
    public LoaderService(ReportService reportService) {
        this.reportService = reportService;
    }

    private String convertSchemeIfNecessary(String scheme) {
        return scheme.replace("h2c", "https").replace("h2", "http");
    }

    private final FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, method, path, Unpooled.buffer(0), headers, new DefaultHttpHeaders());

    private AtomicLong start = new AtomicLong(0L);

    @Async
    @Scheduled(fixedRate = 5_000L)
    public void start() {
        if (start.get() != 0) {
            return;
        } else {
            start.set(-1);
        }
        LOGGER.info("Using " + threads + " thread(s)");

        final EventLoopGroup group = getEventLoopGroup(threads);

        final Proto proto = Proto.schemaToProto(uri.getScheme());
        if (proto == null) {
            LOGGER.error(new IllegalStateException("Proto is NULL"));
            return;
        }

        final AtomicBoolean finished = proto.finished();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.
            group(group).
            channel(getSocketChannelClass()).
            option(ChannelOption.SO_KEEPALIVE, true).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 60000).
            option(ChannelOption.TCP_NODELAY, true).
            option(ChannelOption.SO_REUSEADDR, true);

        Channel[] channels = new Channel[numConn];

        try {
            for (int chanId = 0; chanId < numConn; chanId++) {
                channels[chanId] = newChannel(bootstrap, finished, proto);
            }

            start.set(System.currentTimeMillis());
            group.schedule(() -> finished.set(true), durationSec, TimeUnit.SECONDS);

            // reconnect if necessary
            while (!finished.get()) {
                for (int chanId = 0; chanId < numConn; chanId++) {
                    if (!(channels[chanId].isOpen() && channels[chanId].isActive())) {
                        channels[chanId] = newChannel(bootstrap, finished, proto);
                    }
                }
                TimeUnit.MILLISECONDS.sleep(1L);
            }
            reportService.showReport(start.get());
            reportService.reset();

            CountDownLatch latch = new CountDownLatch(channels.length - 1);
            for (Channel channel : channels) {
                group.execute(() -> {
                    if (channel.isOpen()) {
                        try {
                            channel.closeFuture().sync();
                        } catch (Exception e) {
                            // ignored
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }
            latch.await(5, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (!group.isShuttingDown()) {
                group.shutdownGracefully();
            }
        }
    }

    private Channel newChannel(final Bootstrap bootstrap, AtomicBoolean finished, Proto proto) throws InterruptedException {
        final Channel channel = bootstrap.clone().handler(proto.initializer(reportService)).connect(uri.getHost(), uri.getPort()).sync().channel();
        channel.eventLoop().scheduleAtFixedRate(() -> {
            if (channel.isActive() && !finished.get()) {
                channel.writeAndFlush(request.copy());
            }
        }, 1, 1, TimeUnit.MICROSECONDS);

        return channel;
    }

    private EventLoopGroup getEventLoopGroup(int numCores) {
        // @formatter:off
        return IS_MAC ? new KQueueEventLoopGroup(numCores) :
                IS_LINUX ? new EpollEventLoopGroup(numCores) :
                        new NioEventLoopGroup(numCores);
        // @formatter:on
    }

    private Class<? extends Channel> getSocketChannelClass() {
        // @formatter:off
        return IS_MAC ? KQueueSocketChannel.class :
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
