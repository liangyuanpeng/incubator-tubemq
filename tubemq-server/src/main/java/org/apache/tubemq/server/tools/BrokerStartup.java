/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.tools;

import org.apache.log4j.PropertyConfigurator;
import org.apache.tubemq.server.broker.BrokerConfig;
import org.apache.tubemq.server.broker.TubeBroker;

public class BrokerStartup {
    public static void main(final String[] args) throws Exception {
        PropertyConfigurator.configure("H:\\repo\\git\\incubator-tubemq\\conf\\tools.log4j.properties");
        final String configFilePath = ToolUtils.getConfigFilePath(args);
        final BrokerConfig tubeConfig = ToolUtils.getBrokerConfig(configFilePath);
        final TubeBroker server = new TubeBroker(tubeConfig);
        server.start();
    }
}
