package tuxmonteiro.lab.taurina.services;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.ssl.SslContext;

public class Http1ClientInitializer extends ChannelInitializer<SocketChannel>
{


    private final SslContext sslContext;
    private final ChannelHandler handler;

    public Http1ClientInitializer(SslContext sslContext, ChannelHandler handler) {
        this.sslContext = sslContext;
        this.handler = handler;
    }

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
        final ChannelPipeline pipeline = channel.pipeline();
//                pipeline.addLast(new IdleStateHandler(10, 10, 0, TimeUnit.SECONDS));
        if (sslContext != null) {
            pipeline.addLast(sslContext.newHandler(channel.alloc()));
        }
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new HttpContentDecompressor());
        pipeline.addLast(handler);
    }
}
