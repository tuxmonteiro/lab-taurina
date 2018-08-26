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

package tuxmonteiro.lab.taurina.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import tuxmonteiro.lab.taurina.services.CookieService;
import tuxmonteiro.lab.taurina.services.ReportService;

class Http1ClientHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final ReportService reportService;
    private final CookieService cookieService;

    public Http1ClientHandler(ReportService reportService, CookieService cookieService) {
        this.reportService = reportService;
        this.cookieService = cookieService;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        reportService.connIncr();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        reportService.connDecr();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext channelHandlerContext, HttpObject msg) throws Exception {

        if (msg instanceof HttpResponse) {
            reportService.responseIncr();
            HttpResponse response = (HttpResponse) msg;
            cookieService.loadCookies(response.headers());
            reportService.bodySizeAccumalator(response.toString().length());
        }

        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            ByteBuf byteBuf = content.content();
            if (byteBuf.isReadable()) {
                reportService.bodySizeAccumalator(byteBuf.readableBytes());
            }
        }
    }

}
