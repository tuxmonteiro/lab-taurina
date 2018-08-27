package tuxmonteiro.lab.taurina.nettyutils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
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
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;
import tuxmonteiro.lab.taurina.entity.ReportService;
import tuxmonteiro.lab.taurina.enumerator.Proto;

@Service
public class ChannelManager {

    private static final Log LOGGER = LogFactory.getLog(ChannelManager.class);

    private static final boolean IS_MAC = isMac();
    private static final boolean IS_LINUX = isLinux();
    private ReportService reportService;

    public ChannelManager (ReportService reportService){
        this.reportService = reportService;
    }

    public Class<? extends Channel> getSocketChannelClass() {
        // @formatter:off
        return IS_MAC   ? KQueueSocketChannel.class :
               IS_LINUX ? EpollSocketChannel.class :
                          NioSocketChannel.class;
        // @formatter:on
    }
    public Channel newChannel(final Bootstrap bootstrap, Proto proto, URI uri, FullHttpRequest request,ReportService reportService) {
        try {
            final Channel channel = bootstrap
                .clone()
                .handler(initializer(reportService,proto))
                .connect(uri.getHost(), uri.getPort())
                .sync()
                .channel() ;
            channel.eventLoop().scheduleAtFixedRate(() -> {
                if (channel.isActive()) {
                    reportService.writeAsyncIncr();
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

    public synchronized void activeChanels(int numConn, final Proto proto, final Bootstrap bootstrap, final Channel[] channels, URI uri, FullHttpRequest request,ReportService reportService) {
        for (int chanId = 0; chanId < numConn; chanId++) {
            if (channels[chanId] == null || !channels[chanId].isActive()) {
                Channel channel = newChannel(bootstrap, proto, uri, request,reportService);
                if (channel != null) {
                    channels[chanId] = channel;
                }
            }
        }
    }


    public void closeChannels(EventLoopGroup group, Channel[] channels, int timeout, TimeUnit unit) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(channels.length - 1);
        for (Channel channel : channels) {
            group.execute(() -> {
                closeChannel(latch, channel);
            });
        }
        latch.await(timeout, unit);
    }


    public void closeChannel(final CountDownLatch latch, final Channel channel) {
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

    public EventLoopGroup getEventLoopGroup(int numCores) {
        // @formatter:off
        return IS_MAC   ? new KQueueEventLoopGroup(numCores) :
               IS_LINUX ? new EpollEventLoopGroup(numCores) :
                          new NioEventLoopGroup(numCores);
        // @formatter:on
    }

    public static String getOS() {
        return System.getProperty("os.name", "UNDEF").toLowerCase();
    }

    public static boolean isMac() {
        boolean result = getOS().startsWith("mac");
        if (result) {
            LOGGER.warn("Hello. I'm Mac");
        }
        return result;
    }

    public static boolean isLinux() {
        boolean result = getOS().startsWith("linux");
        if (result) {
            LOGGER.warn("Hello. I'm Linux");
        }
        return result;
    }

    public void reconnectIfNecessary(int numConn, final Proto proto, final EventLoopGroup group, Bootstrap bootstrap, Channel[] channels, URI uri, FullHttpRequest request,final ReportService reportService) {
        group.scheduleAtFixedRate(() ->
          activeChanels(numConn, proto, bootstrap, channels, uri, request,reportService), 100, 100, TimeUnit.MICROSECONDS);
    }


    public SslContext sslContext(Boolean ssl) {
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

    public ChannelInitializer initializer(final ReportService reportService,Proto proto) {
        if (proto == Proto.HTTP_2) {
            return new Http2ClientInitializer(sslContext(Boolean.FALSE), Integer.MAX_VALUE, reportService);
        }

        if (proto == Proto.HTTPS_2) {
            return new Http2ClientInitializer(sslContext(Boolean.TRUE), Integer.MAX_VALUE, reportService);
        }

        if (proto == Proto.HTTPS_1) {
            return new Http2ClientInitializer(sslContext(Boolean.TRUE), Integer.MAX_VALUE, reportService);
        }
        return new Http1ClientInitializer(sslContext(Boolean.FALSE), reportService);
    }


    public ReportService getReportService() {
        return reportService;
    }

    public void setReportService(ReportService reportService) {
        this.reportService = reportService;
    }
}
