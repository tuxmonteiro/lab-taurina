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
import io.netty.handler.codec.http.HttpHeaderNames;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;
import tuxmonteiro.lab.taurina.common.Proto;
import tuxmonteiro.lab.taurina.entity.AuthProperty;
import tuxmonteiro.lab.taurina.entity.RequestProperty;
import tuxmonteiro.lab.taurina.entity.RootProperty;
import tuxmonteiro.lab.taurina.handler.Http1ClientInitializer;
import tuxmonteiro.lab.taurina.handler.Http2ClientInitializer;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

@Service
@EnableAsync
@EnableScheduling
public class LoaderService {

    @Bean
    public TaskExecutor taskExecutor() {
        return new TaskExecutorAdapter(Executors.newSingleThreadExecutor());
    }

    private final ReportService reportService;
    private final CookieService cookieService;

    private static final Log LOGGER = LogFactory.getLog(LoaderService.class);
    private static final boolean IS_MAC = isMac();
    private static final boolean IS_LINUX = isLinux();

    private long schedPeriod = 50L;

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
    public void start(final RootProperty rootProperty) {
        EventLoopGroup group = null;
        try {

            final TreeSet<RequestProperty> requestsProperties = requestsProperty(rootProperty);
            rootProperty.setRequests(requestsProperties);
            rootProperty.setUri(null);
            rootProperty.setMethod(null);
            rootProperty.setHeaders(null);

            LOGGER.info(rootProperty);

            AtomicReference<String> scheme = new AtomicReference<>(null);
            final FullHttpRequest[] requests = convertPropertyToHttpRequest(requestsProperties, scheme);
            if (scheme.get() == null) {
                LOGGER.error("Scheme not initialized");
                return;
            }

            int numConn = rootProperty.getNumConn() / rootProperty.getParallelLoaders();
            @SuppressWarnings("deprecation")
            int durationSec = Optional.ofNullable(rootProperty.getDurationTimeSec())
                    .orElse(rootProperty.getDurationTimeMillis() / 1000);
            int threads = rootProperty.getThreads();

            LOGGER.info("Using " + threads + " thread(s)");

            cookieService.saveCookies(rootProperty.getSaveCookies());
            group = getEventLoopGroup(threads);
            final Proto proto = Proto.valueOf(scheme.get().toUpperCase());
            final Bootstrap bootstrap = newBootstrap(group);
            Channel[] channels = new Channel[numConn];
            double lastPerformanceRate = reportService.lastPerformanceRate();
            schedPeriod = Math.max(10L, (long) (schedPeriod * lastPerformanceRate / 1.05));

            LOGGER.info("Sched Period: " + schedPeriod + " us");

            activeChanels(numConn, proto, bootstrap, channels, requests, schedPeriod);

            start.set(System.currentTimeMillis());

            boolean forceReconnect = rootProperty.getForceReconnect();
            reconnectIfNecessary(forceReconnect, numConn, proto, group, bootstrap, channels, requests, schedPeriod);

            TimeUnit.SECONDS.sleep(durationSec);

            reportService.showReport(start.get());
            reportService.reset();
            cookieService.reset();

            closeChannels(group, channels, 10, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(e.getMessage(), e);
            }
        } finally {
            if (group != null && !group.isShuttingDown()) {
                group.shutdownGracefully();
            }
        }
    }

    private FullHttpRequest[] convertPropertyToHttpRequest(final TreeSet<RequestProperty> requestsProperties, final AtomicReference<String> scheme) {
        final FullHttpRequest[] requests = new FullHttpRequest[requestsProperties.size()];
        int requestId = 0;
        for (RequestProperty requestProperty: requestsProperties) {
            final URI uri = URI.create(requestProperty.getUri());
            if (scheme.get() == null) {
                scheme.set(uri.getScheme());
            }
            final HttpHeaders headers = new DefaultHttpHeaders()
                    .add(HOST, uri.getHost() + (uri.getPort() > 0 ? ":" + uri.getPort() : ""))
                    .add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), convertSchemeIfNecessary(uri.getScheme()));
            Optional.ofNullable(requestProperty.getHeaders()).orElse(Collections.emptyMap()).forEach(headers::add);
            AuthProperty authProperty = Optional.ofNullable(requestProperty.getAuth()).orElse(new AuthProperty());
            final String credentials = authProperty.getCredentials();
            if (credentials != null && !credentials.isEmpty()) {
                headers.add(HttpHeaderNames.AUTHORIZATION,
                        "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(Charset.defaultCharset())));
            }

            HttpMethod method = HttpMethod.valueOf(requestProperty.getMethod());
            String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
            final String bodyStr = requestProperty.getBody();
            ByteBuf body = bodyStr != null && !bodyStr.isEmpty() ?
                    Unpooled.copiedBuffer(bodyStr.getBytes(Charset.defaultCharset())) : Unpooled.buffer(0);

            requests[requestId] = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, body, headers, new DefaultHttpHeaders());
            requestId++;
        }
        return requests;
    }

    private TreeSet<RequestProperty> requestsProperty(RootProperty properties) {
        RequestProperty singleRequestProperties = new RequestProperty();
        String uriStr = properties.getUri();
        boolean singleRequest;
        if (singleRequest = (uriStr != null && !uriStr.isEmpty())) {
            singleRequestProperties.setOrder(0);
            singleRequestProperties.setUri(uriStr);
            singleRequestProperties.setMethod(properties.getMethod());
            singleRequestProperties.setBody(properties.getBody());
            singleRequestProperties.setAuth(properties.getAuth());
            singleRequestProperties.setHeaders(properties.getHeaders());
            singleRequestProperties.setSaveCookies(properties.getSaveCookies());
        }
        return singleRequest ? new TreeSet<RequestProperty>(){{add(singleRequestProperties);}} : properties.getRequests();
    }

    private void reconnectIfNecessary(boolean reconnect, int numConn, final Proto proto, final EventLoopGroup group, Bootstrap bootstrap, Channel[] channels, final FullHttpRequest[] requests, long schedPeriod) {
        if (reconnect) {
            group.scheduleAtFixedRate(() ->
                    activeChanels(numConn, proto, bootstrap, channels, requests, schedPeriod), 100, 100, TimeUnit.MICROSECONDS);
        }
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
                channel.close().await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(e.getMessage(), e);
                }
            } finally {
                latch.countDown();
            }
        }
    }

    private synchronized void activeChanels(int numConn, final Proto proto, final Bootstrap bootstrap, final Channel[] channels, final FullHttpRequest[] requests, long schedPeriod) {
        for (int chanId = 0; chanId < numConn; chanId++) {
            if (channels[chanId] == null || !channels[chanId].isActive()) {

                Channel channel = newChannel(bootstrap, proto, requests, schedPeriod);
                if (channel != null) {
                    channels[chanId] = channel;
                }
            }
        }
    }

    private Channel newChannel(final Bootstrap bootstrap, Proto proto, final FullHttpRequest[] requests, long schedPeriod) {
        try {
            URI uri = URI.create(proto.name().toLowerCase() + "://" + requests[0].headers().get(HttpHeaderNames.HOST) + requests[0].uri());
            final Channel channel = bootstrap
                                        .clone()
                                        .handler(initializer(proto))
                                        .connect(uri.getHost(), uri.getPort())
                                        .sync()
                                        .channel();
            channel.eventLoop().scheduleAtFixedRate(() -> {
                if (channel.isActive()) {
                    for (FullHttpRequest request: requests) {
                        reportService.writeCounterIncr();
                        cookieService.applyCookies(request.headers());
                        channel.writeAndFlush(request.copy());
                    }
                }
            }, schedPeriod, schedPeriod, TimeUnit.MICROSECONDS);
            return channel;
        } catch (InterruptedException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(e.getMessage(), e);
            }
        }
        return null;
    }

    private SslContext sslContext(boolean ssl) {
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

    private ChannelInitializer initializer(Proto proto) {
        if (proto == Proto.H2 || proto == Proto.H2C) {
            return new Http2ClientInitializer(sslContext(proto.isSsl()), Integer.MAX_VALUE, reportService, cookieService);
        }
        return new Http1ClientInitializer(sslContext(proto.isSsl()), reportService, cookieService);
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
