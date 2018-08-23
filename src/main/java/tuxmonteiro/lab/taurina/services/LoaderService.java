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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.*;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.net.ssl.SSLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

@Service
@EnableAsync
@EnableScheduling
public class LoaderService {

    enum Proto {
        HTTPS_1(true, Object.class),
        HTTPS_2(true, Object.class),
        HTTP_1(false, Object.class),
        HTTP_2(false, Object.class);

        private final boolean ssl;
        private final Class<Object> aClass;
        private final AtomicBoolean finished;

        Proto(boolean ssl, Class<Object> aClass) {
            this.finished = new AtomicBoolean(false);
            this.ssl = ssl;
            this.aClass = aClass;
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
                    final SslContext sslContext = SslContextBuilder.forClient()
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
                    return sslContext;
                } catch (SSLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            return null;
        }

        public ChannelInitializer initializer() {
            if (this == HTTP_2 || this == HTTPS_2) {
                return new Http2ClientInitializer(sslContext(), Integer.MAX_VALUE);
            }
            return new Http1ClientInitializer(sslContext(), new MyHandler(finished()));
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
    private final String uriStr = System.getProperty("taurina.uri", "https://127.0.0.1:8443");
    private final URI uri = URI.create(uriStr);
    private final String path = System.getProperty("taurina.targetpath", "/");
    private final int threads = Integer.parseInt(System.getProperty("taurina.threads",
        String.valueOf(NUM_CORES > numConn ? numConn : NUM_CORES)));

    private final HttpHeaders headers = new DefaultHttpHeaders().add(HOST, uri.getHost() + (uri.getPort() > 0 ? ":" + uri.getPort() : ""));
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
        ChannelInitializer initializer = proto.initializer();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.
            group(group).
            channel(getSocketChannelClass()).
            option(ChannelOption.SO_KEEPALIVE, true).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 60000).
            option(ChannelOption.TCP_NODELAY, true).
            option(ChannelOption.SO_REUSEADDR, true).
            handler(initializer);

        Channel[] channels = new Channel[numConn];

        try {
            for (int chanId = 0; chanId < numConn; chanId++) {
                channels[chanId] = newChannel(bootstrap, finished);
            }

            start.set(System.currentTimeMillis());
            group.schedule(() -> finished.set(true), durationSec, TimeUnit.SECONDS);

            // reconnect if necessary
            while (!finished.get()) {
                for (int chanId = 0; chanId < numConn; chanId++) {
                    if (!(channels[chanId].isOpen() && channels[chanId].isActive())) {
                        channels[chanId] = newChannel(bootstrap, finished);
                    }
                }
                TimeUnit.MILLISECONDS.sleep(1L);
            }

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

    private Channel newChannel(final Bootstrap bootstrap, AtomicBoolean finished) throws InterruptedException {
        final Channel channel = bootstrap.clone().connect(uri.getHost(), uri.getPort()).sync().channel();
        channel.eventLoop().scheduleAtFixedRate(() -> {
            if (channel.isActive() && !finished.get()) {
                channel.writeAndFlush(request.copy());
            }
        }, 1, 1, TimeUnit.MICROSECONDS);

        return channel;
    }

    @Sharable
    private static class MyHandler extends SimpleChannelInboundHandler<HttpObject> {

        private static final AtomicInteger RESPONSE_COUNTER = new AtomicInteger(0);
        private static final AtomicInteger CHANNELS_ACTIVE = new AtomicInteger(0);
        private static final AtomicLong TOTAL_SIZE = new AtomicLong(0L);

        private final AtomicBoolean finished;

        public MyHandler(AtomicBoolean finished) {
            this.finished = finished;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            CHANNELS_ACTIVE.incrementAndGet();
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            CHANNELS_ACTIVE.decrementAndGet();
            super.channelInactive(ctx);
        }

        @Override
        public void channelRead0(ChannelHandlerContext channelHandlerContext, HttpObject msg) throws Exception {

            if (msg instanceof HttpResponse) {
                RESPONSE_COUNTER.incrementAndGet();
                HttpResponse response = (HttpResponse) msg;
                TOTAL_SIZE.addAndGet(response.toString().length());
            }

            if (msg instanceof FullHttpResponse) {
                RESPONSE_COUNTER.incrementAndGet();
                HttpResponse response = (FullHttpResponse) msg;
                TOTAL_SIZE.addAndGet(response.toString().length());
            }

            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                ByteBuf byteBuf = content.content();
                if (byteBuf.isReadable()) {
                    TOTAL_SIZE.addAndGet(byteBuf.readableBytes());
                }
                if (content instanceof LastHttpContent && finished.get()) {
                    channelHandlerContext.close();
                }
            }
        }

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
