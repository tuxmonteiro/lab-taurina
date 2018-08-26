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

package tuxmonteiro.lab.taurina.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import tuxmonteiro.lab.taurina.entity.RootProperty;
import tuxmonteiro.lab.taurina.services.LoaderService;

@RestController
public class TestController {

    private final LoaderService loaderService;

    @Autowired
    public TestController(LoaderService loaderService) {
        this.loaderService = loaderService;
    }

    @PostMapping(value = "/test", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public ResponseEntity<Void> post(@RequestBody RootProperty rootProperty) {
        loaderService.start(rootProperty);
        return ResponseEntity.accepted().build();
    }
}
