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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tuxmonteiro.lab.taurina.entity.ReportService;
import tuxmonteiro.lab.taurina.enumerator.Proto;
import tuxmonteiro.lab.taurina.nettyutils.ChannelManager;

@Service
@EnableAsync
@EnableScheduling
public class LoaderService {

    private final ReportService reportService;

    private static final Log LOGGER = LogFactory.getLog(LoaderService.class);

    private static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

    private final ObjectMapper mapper = new ObjectMapper();


    private ChannelManager channelManager;

    @Autowired
    public LoaderService(ReportService reportService) {
        this.reportService = reportService;
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

        channelManager = new ChannelManager(reportService);
        EventLoopGroup group = null;
        try {
            String jsonStr = "{\"uri\":\"http://127.0.0.1:8030\", \"method\":\"POST\", \"body\" :\"\" }";

            HashMap hashMap = mapper.readValue(jsonStr, HashMap.class);

            String uriStr = (String) hashMap.get("uri");
            String methodStr = Optional.ofNullable((String) hashMap.get("method")).orElse("GET");
            HttpMethod method = HttpMethod.valueOf(methodStr);
            int numConn = Optional.ofNullable((Integer) hashMap.get("numConn")).orElse(10);
            int durationSec = Optional.ofNullable((Integer) hashMap.get("durationTimeMillis")).orElse(30000) / 1000;
            URI uriFromJson = URI.create(uriStr);
            String pathFromURI = uriFromJson.getRawPath();
            String path = pathFromURI == null || pathFromURI.isEmpty() ? "/" : pathFromURI;

            String bodyStr = (String) hashMap.get("body");
            ByteBuf body = Unpooled.copiedBuffer(bodyStr == null ? new byte[0] : bodyStr.getBytes());

            Map auth =  Optional.ofNullable((Map) hashMap.get("auth")).orElse(Collections.emptyMap());

            final HttpHeaders headers = new DefaultHttpHeaders()
                .add(HOST, uriFromJson.getHost() + (uriFromJson.getPort() > 0 ? ":" + uriFromJson.getPort() : ""))
                .add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), convertSchemeIfNecessary(uriFromJson.getScheme()));

            String credentials = (String) auth.get("credentials");

            if(credentials != null){
               String a =  Base64.getEncoder().encode(credentials.getBytes()).toString();
                headers.add(HttpHeaderNames.AUTHORIZATION ,"Basic".concat(a));
            }

            // TODO: Check cast
            @SuppressWarnings("unchecked")
            Map<String, String> headersFromJson = Optional.ofNullable((Map<String, String>) hashMap.get("headers")).orElse(Collections.emptyMap());
            headersFromJson.forEach(headers::add);

            final FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, path, body, headers, new DefaultHttpHeaders());
            int threads = Integer.parseInt(System.getProperty("taurina.threads",
                String.valueOf(NUM_CORES > numConn ? numConn : NUM_CORES)));

            LOGGER.info("Using " + threads + " thread(s)");

            group = channelManager.getEventLoopGroup(threads);
            final Proto proto = Proto.schemaToProto(uriFromJson.getScheme());
            final Bootstrap bootstrap = newBootstrap(group);

            Channel[] channels = new Channel[numConn];
            channelManager.activeChanels(numConn, proto, bootstrap, channels, uriFromJson, request);

            start.set(System.currentTimeMillis());

            channelManager.reconnectIfNecessary(numConn, proto, group, bootstrap, channels, uriFromJson, request);

            TimeUnit.SECONDS.sleep(durationSec);

            reportService.showReport(start.get());
            reportService.reset();

            channelManager.closeChannels(group, channels, 5, TimeUnit.SECONDS);

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

    private Bootstrap newBootstrap(EventLoopGroup group) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.
            group(group).
            channel(channelManager.getSocketChannelClass()).
            option(ChannelOption.SO_KEEPALIVE, true).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 60000).
            option(ChannelOption.TCP_NODELAY, true).
            option(ChannelOption.SO_REUSEADDR, true);
        return bootstrap;
    }
}