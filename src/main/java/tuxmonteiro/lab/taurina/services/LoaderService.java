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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

@Service
public class LoaderService {

    private static final Log LOGGER = LogFactory.getLog(LoaderService.class);

    private static final boolean IS_MAC   = isMac();
    private static final boolean IS_LINUX = isLinux();

    private static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicLong totalSize = new AtomicLong(0L);
    private final AtomicInteger responseCounter = new AtomicInteger(0);
    private final AtomicInteger channelsActive = new AtomicInteger(0);

    private final int numConn = Integer.parseInt(System.getProperty("taurina.numconn", "10"));
    private final int durationSec = Integer.parseInt(System.getProperty("taurina.duration", "30"));
    private final HttpMethod method = HttpMethod.GET;
    private final String host = System.getProperty("taurina.targethost", "127.0.0.1");
    private final int port = Integer.parseInt(System.getProperty("taurina.targetport", "8030"));
//    private final int port = Integer.parseInt(System.getProperty("taurina.targetport", "8040"));
//    private final String path = System.getProperty("taurina.targetpath", "/base64/QQ%3D%3D");
    private final String path = System.getProperty("taurina.targetpath", "/");
    private final int threads = Integer.parseInt(System.getProperty("taurina.threads",
                                        String.valueOf(NUM_CORES > numConn ? numConn : NUM_CORES)));

    private final HttpHeaders headers = new DefaultHttpHeaders().add(HOST, host + (port > 0 ? ":" + port : ""));
    private final FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, method, path, Unpooled.buffer(0), headers, new DefaultHttpHeaders());

    private AtomicLong start = new AtomicLong(0L);

    @PostConstruct
    public void start() {
        LOGGER.info("Using " + threads + " thread(s)");

        final EventLoopGroup group = getEventLoopGroup(threads);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.
                group(group).
                channel(getSocketChannelClass()).
                option(ChannelOption.SO_KEEPALIVE, true).
                option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 60000).
                option(ChannelOption.TCP_NODELAY, true).
                option(ChannelOption.SO_REUSEADDR, true).
                handler(initializer());

        Channel[] channels = new Channel[numConn];

        try {
            for (int chanId = 0; chanId < numConn; chanId++) {
                newChannel(bootstrap, channels, chanId);
            }

            start.set(System.currentTimeMillis());
            group.schedule(() -> finished.set(true), durationSec, TimeUnit.SECONDS);

            while (!finished.get()) {
                for (int chanId = 0; chanId < numConn; chanId++) {
                    if (!(channels[chanId].isOpen() && channels[chanId].isActive())) {
                        newChannel(bootstrap, channels, chanId);
                    }
                }
                TimeUnit.MILLISECONDS.sleep(100L);
            }

            if (channelsActive.get() < numConn) {
                LOGGER.error(">--> channels actives: " + channelsActive.get());
            }

            long totalTime = (System.currentTimeMillis() - start.get()) / 1_000L;
            int responseTotal = responseCounter.get();
            long size = totalSize.get();

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

            LOGGER.warn(">>> " +
                    "total time (s): " + totalTime);
            LOGGER.warn(">>> " +
                    "total responses: " + responseTotal + " / totalSize: " + (size / 1024));
            LOGGER.warn(">>> " +
                    "avg response: " + responseTotal / totalTime + " / " +
                    "avg size KB/s: " + (size / 1024) / totalTime);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (!group.isShuttingDown()) {
                group.shutdownGracefully();
            }
        }
    }

    private void newChannel(final Bootstrap bootstrap, final Channel[] channels, int chanId) throws InterruptedException {
        final Bootstrap clone = bootstrap.clone();
        final Channel chan = channels[chanId] = clone.connect(host, port).sync().channel();
        final EventLoop eventLoop = channels[chanId].eventLoop();
        eventLoop.scheduleAtFixedRate(() -> {
            if (!finished.get()) {
                chan.writeAndFlush(request.copy());
            }
        }, 1, 1, TimeUnit.MICROSECONDS);
    }

    private static class MyHandler extends SimpleChannelInboundHandler<HttpObject> {

        private final AtomicInteger responseCounter;
        private final AtomicInteger channelsActive;
        private final AtomicLong totalSize;
        private final AtomicBoolean finished;

        MyHandler(final AtomicInteger responseConter, AtomicInteger channelsActive, AtomicLong totalSize, AtomicBoolean finished) {
            this.responseCounter = responseConter;
            this.channelsActive = channelsActive;
            this.totalSize = totalSize;
            this.finished = finished;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channelsActive.incrementAndGet();
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            channelsActive.decrementAndGet();
            super.channelInactive(ctx);
        }

        @Override
        public void channelRead0(ChannelHandlerContext channelHandlerContext, HttpObject msg) throws Exception {
            if (msg instanceof HttpResponse) {
                responseCounter.incrementAndGet();
                HttpResponse response = (HttpResponse) msg;
                totalSize.addAndGet(response.toString().length());
            }
            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                ByteBuf byteBuf = content.content();
                if (byteBuf.isReadable()) {
                    totalSize.addAndGet(byteBuf.readableBytes());
                }
                if (content instanceof LastHttpContent && finished.get()) {
                    channelHandlerContext.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.error(cause.getMessage(), cause);
        }
    }

    private ChannelInitializer<SocketChannel> initializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();
//                pipeline.addLast(new IdleStateHandler(10, 10, 0, TimeUnit.SECONDS));
                pipeline.addLast(new HttpClientCodec());
//                pipeline.addLast(new HttpContentDecompressor());
                pipeline.addLast("myinbound", new MyHandler(responseCounter, channelsActive, totalSize, finished));
            }
        };
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
            LOGGER.warn("I'm Mac");
        }
        return result;
    }

    private static boolean isLinux() {
        boolean result = getOS().startsWith("linux");
        if (result) {
            LOGGER.warn("I'm Linux");
        }
        return result;
    }

}
