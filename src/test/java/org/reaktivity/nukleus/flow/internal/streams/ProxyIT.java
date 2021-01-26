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
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class ProxyIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/flow/control/route")
            .addScriptRoot("nukleus", "org/reaktivity/specification/nukleus/flow/streams/proxy")
            .addScriptRoot("target", "org/reaktivity/specification/flow/proxy");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .nukleus("flow"::equals)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/client.connected/client",
        "${target}/client.connected/server"})
    public void shouldConnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/client.sent.data/client",
        "${target}/client.sent.data/server"})
    public void shouldReceiveClientSentData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/client.sent.flush/client",
        "${target}/client.sent.flush/server"})
    public void shouldReceiveClientSentFlush() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/client.sent.challenge/client",
        "${target}/client.sent.challenge/server"})
    public void shouldReceiveClientSentChallenge() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/client.received.data/client",
        "${target}/client.received.data/server"})
    public void shouldReceiveServerSentData() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/server.sent.flush/client",
        "${target}/server.sent.flush/server"})
    public void shouldReceiveServerSentFlush() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/server.sent.challenge/client",
        "${target}/server.sent.challenge/server"})
    public void shouldReceiveServerSentChallenge() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/client.received.data/client",
        "${target}/client.received.data/server"})
    @Configure(name = "reaktor.buffer.slot.capacity", value = "16")
    public void shouldReceiveClientSentDataWithLimitedBuffer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${nukleus}/client.sent.and.received.data/client",
        "${target}/client.sent.and.received.data/server"})
    public void shouldReceiveClientAndServerSentData() throws Exception
    {
        k3po.finish();
    }
}
