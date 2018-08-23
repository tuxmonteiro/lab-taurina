package tuxmonteiro.lab.taurina.services;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.PlatformDependent;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Http2ResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

    private static final Log LOGGER = LogFactory.getLog(LoaderService.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        Integer streamId = msg.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
        if (streamId == null) {
            LOGGER.error("HttpResponseHandler unexpected message received: " + msg);
            return;
        }

        // Do stuff with the message (for now just print it)
        final ByteBuf content = msg.content();
        if (content.isReadable()) {
            int contentLength = content.readableBytes();
            byte[] arr = new byte[contentLength];
            content.readBytes(arr);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("stream_id=" + streamId + " : " + new String(arr, 0, contentLength, CharsetUtil.UTF_8));
            }
        }

    }
}