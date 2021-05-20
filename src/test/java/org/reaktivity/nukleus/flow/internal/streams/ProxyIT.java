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
package org.reaktivity.nukleus.flow.internal.streams;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;
import org.reaktivity.reaktor.test.annotation.Configure;

public class ProxyIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("client", "org/reaktivity/specification/nukleus/flow/streams/client")
        .addScriptRoot("server", "org/reaktivity/specification/nukleus/flow/streams/server");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configurationRoot("org/reaktivity/specification/nukleus/flow/config")
        .external("server#0")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/client.connected/client",
        "${server}/client.connected/server"})
    public void shouldConnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/client.sent.data/client",
        "${server}/client.sent.data/server"})
    public void shouldReceiveClientSentData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/client.sent.flush/client",
        "${server}/client.sent.flush/server"})
    public void shouldReceiveClientSentFlush() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/client.sent.challenge/client",
        "${server}/client.sent.challenge/server"})
    public void shouldReceiveClientSentChallenge() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/client.received.data/client",
        "${server}/client.received.data/server"})
    public void shouldReceiveServerSentData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/server.sent.flush/client",
        "${server}/server.sent.flush/server"})
    public void shouldReceiveServerSentFlush() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/server.sent.challenge/client",
        "${server}/server.sent.challenge/server"})
    public void shouldReceiveServerSentChallenge() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/client.received.data/client",
        "${server}/client.received.data/server"})
    @Configure(name = "reaktor.buffer.slot.capacity", value = "16")
    public void shouldReceiveClientSentDataWithLimitedBuffer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("proxy.json")
    @Specification({
        "${client}/client.sent.and.received.data/client",
        "${server}/client.sent.and.received.data/server"})
    public void shouldReceiveClientAndServerSentData() throws Exception
    {
        k3po.finish();
    }
}
