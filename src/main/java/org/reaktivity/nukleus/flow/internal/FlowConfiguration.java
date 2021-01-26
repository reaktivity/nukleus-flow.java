/**
 * Copyright 2016-2021 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.flow.internal;

import org.reaktivity.nukleus.Configuration;

public class FlowConfiguration extends Configuration
{
    public static final IntPropertyDef FLOW_MAXIMUM_SIGNALS;

    private static final ConfigurationDef FLOW_CONFIG;

    static
    {
        ConfigurationDef config = new ConfigurationDef(String.format("nukleus.%s", FlowNukleus.NAME));
        FLOW_MAXIMUM_SIGNALS = config.property("maximum.signals", Integer.MAX_VALUE);
        FLOW_CONFIG = config;
    }

    public FlowConfiguration(
        Configuration config)
    {
        super(FLOW_CONFIG, config);
    }

    public int maximumSignals()
    {
        return FLOW_MAXIMUM_SIGNALS.getAsInt(this);
    }
}
