package tuxmonteiro.lab.taurina.services;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class Http1ClientHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final AtomicInteger RESPONSE_COUNTER = new AtomicInteger(0);
    private static final AtomicInteger CHANNELS_ACTIVE = new AtomicInteger(0);
    private static final AtomicLong TOTAL_SIZE = new AtomicLong(0L);

    private final AtomicBoolean finished;

    public Http1ClientHandler(AtomicBoolean finished) {
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
