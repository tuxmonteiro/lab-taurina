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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

@Service
public class LoaderService {

    private static final Log LOGGER = LogFactory.getLog(LoaderService.class);

    private final AtomicLong start = new AtomicLong(0);
    private final AttributeKey<AtomicLong> attrTotalSize = AttributeKey.newInstance("totalSize");
    private final AttributeKey<AtomicInteger> attrResponseCounter = AttributeKey.newInstance("responseCounter");
    private final AttributeKey<AtomicBoolean> attrFinished = AttributeKey.newInstance("finished");
    final AtomicBoolean finished = new AtomicBoolean(false);
    final AtomicLong totalSize = new AtomicLong(0L);
    final AtomicInteger responseCounter = new AtomicInteger(0);

    private final int numConn = 10;
    private final int durationSec = 30;
    private final HttpMethod method = HttpMethod.GET;
    private final String host = "127.0.0.1";
    private final int port = 3000;
    private final String path = "/";

    private final HttpHeaders headers = new DefaultHttpHeaders().add(HOST, host + (port > 0 ? ":" + port : ""));
    private final FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, method, path, Unpooled.buffer(0), headers, new DefaultHttpHeaders());

    @PostConstruct
    public void start() {
        Bootstrap bootstrap = new Bootstrap();
        final EventLoopGroup group = new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors());


        bootstrap.
                group(new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors())).
                channel(EpollSocketChannel.class).
                option(ChannelOption.SO_KEEPALIVE, true).
                option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000).
//                attr(attrTotalSize, totalSize).
//                attr(attrResponseCounter, responseCounter).
//                attr(attrFinished, finished).
                handler(initializer());

        Channel[] channels = new Channel[numConn];

        try {
            for(int chanId = 0; chanId < numConn; chanId++) {
                Bootstrap clone = bootstrap.clone();
                channels[chanId] = clone.connect(host, port).sync().channel();
            }

            new ScheduledThreadPoolExecutor(1).schedule(() -> finished.set(true), durationSec, TimeUnit.SECONDS);
            start.set(System.currentTimeMillis());
            while (System.currentTimeMillis() - start.get() < durationSec * 1000L) {
                final CountDownLatch latch = new CountDownLatch(channels.length - 1);
                for (Channel channel : channels) {
                    channel.writeAndFlush(request.copy(), new DefaultChannelPromise(channel).addListener((future -> latch.countDown())));
                }
                latch.await(1, TimeUnit.SECONDS);
            }
            for (Channel channel: channels) {
                channel.closeFuture().sync();
            }
            long totalTime = (System.currentTimeMillis() - start.get()) / 1000;
            LOGGER.warn(">>> " +
                    "total time (s): " + totalTime);
            LOGGER.warn(">>> " +
                    "responseCounter: " + responseCounter.get() + " / totalSize: " + (totalSize.get() / 1024));
            LOGGER.warn(">>> " +
                    "avg response: " + responseCounter.get() / totalTime + " / " +
                    "avg size KB/s: " + (totalSize.get() / 1024) / totalTime);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (!group.isShuttingDown()) {
                group.shutdownGracefully();
            }
        }
    }

    private ChannelInitializer<SocketChannel> initializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast(new IdleStateHandler(10, 10, 0, TimeUnit.SECONDS));
                pipeline.addLast(new HttpClientCodec());
                pipeline.addLast(new HttpContentDecompressor());
                pipeline.addLast("inbound", new ChannelInboundHandlerAdapter(){

//                    private AtomicInteger responseCounter = null;
//                    private AtomicLong totalSize = null;
//                    private AtomicBoolean finished = null;
//
//                    @Override
//                    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
//                        responseCounter = ctx.channel().attr(attrResponseCounter).get();
//                        totalSize = ctx.channel().attr(attrTotalSize).get();
//                        finished = ctx.channel().attr(attrFinished).get();
//
//                        super.channelRegistered(ctx);
//                    }

                    @Override
                    public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
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
                                channelHandlerContext.channel().close();
                            }
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        LOGGER.error(cause.getMessage(), cause);
                        super.exceptionCaught(ctx, cause);
                    }
                });
            }
        };
    }
}
