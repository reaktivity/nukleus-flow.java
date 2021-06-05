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
package org.reaktivity.nukleus.flow.internal.stream;

import static org.reaktivity.reaktor.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.flow.internal.FlowConfiguration;
import org.reaktivity.nukleus.flow.internal.types.OctetsFW;
import org.reaktivity.nukleus.flow.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.flow.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.flow.internal.types.stream.ChallengeFW;
import org.reaktivity.nukleus.flow.internal.types.stream.DataFW;
import org.reaktivity.nukleus.flow.internal.types.stream.EndFW;
import org.reaktivity.nukleus.flow.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.flow.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.flow.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.flow.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.buffer.BufferPool;
import org.reaktivity.reaktor.nukleus.concurrent.Signaler;
import org.reaktivity.reaktor.nukleus.function.MessageConsumer;
import org.reaktivity.reaktor.nukleus.stream.StreamFactory;

public final class FlowProxyFactory implements FlowStreamFactory
{
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final FlushFW flushRO = new FlushFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final SignalFW signalRO = new SignalFW();
    private final ChallengeFW challengeRO = new ChallengeFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final ChallengeFW.Builder challengeRW = new ChallengeFW.Builder();

    private final BufferPool bufferPool;
    private final MutableDirectBuffer writeBuffer;
    private final StreamFactory streamFactory;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final Signaler signaler;
    private final Long2ObjectHashMap<Binding> bindings;

    private final int maximumSignals;

    public FlowProxyFactory(
        FlowConfiguration config,
        ElektronContext context)
    {
        this.bufferPool = context.bufferPool();
        this.writeBuffer = context.writeBuffer();
        this.streamFactory = context.streamFactory();
        this.supplyInitialId = context::supplyInitialId;
        this.supplyReplyId = context::supplyReplyId;
        this.supplyTraceId = context::supplyTraceId;
        this.signaler = context.signaler();
        this.bindings = new Long2ObjectHashMap<>();
        this.maximumSignals = config.maximumSignals();
    }

    @Override
    public void attach(
        Binding binding)
    {
        bindings.put(binding.id, binding);
    }

    @Override
    public void detach(
        long bindingId)
    {
        bindings.remove(bindingId);
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long routeId = begin.routeId();
        final Binding binding = bindings.get(routeId);

        MessageConsumer newStream = null;

        if (binding != null && binding.exit != null)
        {
            final long initialId = begin.streamId();

            final FlowProxyServer server = new FlowProxyServer(sender, routeId, initialId);
            final FlowProxyClient client = new FlowProxyClient(binding.exit.id);

            server.correlate(client);
            client.correlate(server);

            newStream = server::onMessage;
        }

        return newStream;
    }

    private final class FlowProxyServer
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;

        private FlowProxyClient client;

        private long initialSeq;
        private long initialAck;
        private int initialMax;

        private long replySeq;
        private long replyAck;
        private int replyMax;
        private int replyPad;

        private int initialSlot;
        private int initialSlotOffset;
        private int initialSignals;
        private int remainingSignals;

        private FlowProxyServer(
            MessageConsumer receiver,
            long routeId,
            long initialId)
        {
            this.routeId = routeId;
            this.initialId = initialId;
            this.receiver = receiver;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.initialSlot = NO_SLOT;
        }

        private void correlate(
            FlowProxyClient connect)
        {
            this.client = connect;
        }

        private void onMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onFlush(flush);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                onSignal(signal);
                break;
            case ChallengeFW.TYPE_ID:
                final ChallengeFW challenge = challengeRO.wrap(buffer, index, index + length);
                onChallenge(challenge);
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            final long affinity = begin.affinity();
            final OctetsFW extension = begin.extension();

            initialSeq = sequence;
            initialAck = acknowledge;

            client.begin(sequence, acknowledge, traceId, authorization, affinity, extension);
        }

        private void onData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final int dataSize = data.sizeof();

            final int initialSlotSize = bufferPool.slotCapacity();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence + data.reserved();

            assert initialAck <= initialSeq;

            if (initialSlot == NO_SLOT)
            {
                initialSlot = bufferPool.acquire(replyId);
            }

            if (initialSeq > initialAck + initialMax || initialSlot == NO_SLOT)
            {
                doReject(receiver, routeId, initialId, initialSeq, initialAck, initialMax);
                client.onRejected(traceId);
            }
            else if (initialSlotOffset == 0 && dataSize + Integer.BYTES > initialSlotSize)
            {
                assert initialSlot != NO_SLOT;
                assert initialSlotOffset == 0;

                final int flags = data.flags();
                final long budgetId = data.budgetId();
                final int reserved = data.reserved();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                client.send(initialSeq, traceId, flags, budgetId, reserved, payload, extension);

                bufferPool.release(initialSlot);
                initialSlot = NO_SLOT;
            }
            else
            {
                assert initialSlot != NO_SLOT;

                final MutableDirectBuffer initialBuf = bufferPool.buffer(initialSlot);

                if (initialSlotOffset != 0 && initialSlotOffset + dataSize > initialSlotSize)
                {
                    flush(initialBuf, Integer.BYTES, initialSlotOffset);
                    initialSlotOffset = 0;
                }

                if (initialSlotOffset == 0)
                {
                    final DirectBuffer dataBuf = data.buffer();
                    final int dataOffset = data.offset();
                    final int extensionSize = data.extension().sizeof();

                    initialBuf.putInt(0, extensionSize);
                    initialBuf.putBytes(Integer.BYTES, dataBuf, dataOffset, dataSize);
                    initialSlotOffset += Integer.BYTES + dataSize;
                    remainingSignals = maximumSignals;
                }
                else
                {
                    final OctetsFW fragment = data.payload();
                    final int fragmentReserved = data.reserved();
                    final DirectBuffer fragmentBuf = fragment.buffer();
                    final int fragmentOffset = fragment.offset();
                    final int fragmentSize = fragment.sizeof();
                    final int flagsOffset = Integer.BYTES + DataFW.FIELD_OFFSET_FLAGS;
                    final int flags = initialBuf.getInt(flagsOffset);
                    final int newFlags = flags | (data.flags() & ~0x01) | (data.flags() & 0x02);
                    final int reservedOffset = Integer.BYTES + DataFW.FIELD_OFFSET_RESERVED;
                    final int reserved = initialBuf.getInt(reservedOffset);
                    final int newReserved = reserved + fragmentReserved;
                    final int lengthOffset = Integer.BYTES + DataFW.FIELD_OFFSET_LENGTH;
                    final int length = initialBuf.getInt(lengthOffset);
                    final int newLength = length + fragmentSize;
                    final int extensionSize = initialBuf.getInt(0);
                    final int extensionOffset = initialSlotOffset - extensionSize;
                    final int newExtensionOffset = initialSlotOffset + fragmentSize - extensionSize;

                    initialBuf.putInt(flagsOffset, newFlags);
                    initialBuf.putInt(lengthOffset, newLength);
                    initialBuf.putInt(reservedOffset, newReserved);
                    initialBuf.putBytes(newExtensionOffset, initialBuf, extensionOffset, extensionSize);
                    initialBuf.putBytes(extensionOffset, fragmentBuf, fragmentOffset, fragmentSize);
                    initialSlotOffset += fragmentSize;
                    remainingSignals--;
                }

                if (remainingSignals == 0)
                {
                    flush(initialBuf, Integer.BYTES, initialSlotOffset);

                    bufferPool.release(initialSlot);
                    initialSlot = NO_SLOT;
                    initialSlotOffset = 0;
                }
                else
                {
                    initialSignals++;
                    signaler.signalNow(routeId, replyId, 0);
                }
            }
        }

        private void onFlush(
            FlushFW flush)
        {
            final long sequence = flush.sequence();
            final long traceId = flush.traceId();
            final long authorization = flush.authorization();
            final long budgetId = flush.budgetId();
            final int reserved = flush.reserved();
            final OctetsFW extension = flush.extension();

            if (initialSlot != NO_SLOT)
            {
                assert initialSlot != NO_SLOT;

                final DirectBuffer replyBuf = bufferPool.buffer(initialSlot);
                flush(replyBuf, Integer.BYTES, initialSlotOffset);

                bufferPool.release(initialSlot);
                initialSlot = NO_SLOT;
                initialSlotOffset = 0;
            }

            client.flush(sequence, traceId, authorization, budgetId, reserved, extension);
        }

        private void onEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long traceId = end.traceId();
            final long authorization = end.authorization();
            final OctetsFW extension = end.extension();

            if (initialSlot != NO_SLOT)
            {
                assert initialSlot != NO_SLOT;

                final DirectBuffer replyBuf = bufferPool.buffer(initialSlot);
                flush(replyBuf, Integer.BYTES, initialSlotOffset);

                bufferPool.release(initialSlot);
                initialSlot = NO_SLOT;
                initialSlotOffset = 0;
            }

            client.end(sequence, traceId, authorization, extension);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            final OctetsFW extension = abort.extension();

            client.abort(sequence, traceId, authorization, extension);
        }

        private void onReset(
            ResetFW reset)
        {
            final long acknowledge = reset.acknowledge();
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            final OctetsFW extension = reset.extension();

            client.reset(acknowledge, traceId, authorization, extension);
        }

        private void onWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert sequence <= replySeq;
            assert acknowledge >= replyAck;
            assert maximum >= replyMax;

            this.replyAck = acknowledge;
            this.replyMax = maximum;
            this.replyPad = padding;

            assert replyAck <= replySeq;

            final int replyWindow = replyMax - (int)(replySeq - replyAck);
            if (replyWindow > 0) // threshold = 0
            {
                final long traceId = window.traceId();
                final long budgetId = window.budgetId();

                client.credit(traceId, budgetId, replyWindow, replyPad, replyMax);
            }
        }

        private void onSignal(
            SignalFW signal)
        {
            initialSignals--;

            if (initialSignals == 0 && initialSlot != NO_SLOT)
            {
                assert initialSlot != NO_SLOT;

                final DirectBuffer replyBuf = bufferPool.buffer(initialSlot);
                flush(replyBuf, Integer.BYTES, initialSlotOffset);

                bufferPool.release(initialSlot);
                initialSlot = NO_SLOT;
                initialSlotOffset = 0;
            }
        }

        private void onChallenge(
            ChallengeFW challenge)
        {
            final long acknowledge = challenge.acknowledge();
            final long traceId = challenge.traceId();
            final long authorization = challenge.authorization();
            final OctetsFW extension = challenge.extension();

            client.challenge(acknowledge, traceId, authorization, extension);
        }

        private void onRejected(
            long traceId)
        {
            doRejected(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId);
        }

        private void begin(
            long sequence,
            long acknowledge,
            long traceId,
            long authorization,
            long affinity,
            OctetsFW extension)
        {
            assert acknowledge <= sequence;
            assert sequence >= replySeq;
            assert acknowledge >= replyAck;

            replySeq = sequence;
            replyAck = acknowledge;

            doBegin(receiver, routeId, replyId, replySeq, replyAck, replyMax,
                    traceId, authorization, affinity, extension);
        }

        private void send(
            long sequence,
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            assert sequence >= replySeq;

            replySeq = sequence;

            doData(receiver, routeId, replyId, replySeq, replyAck, replyMax,
                    traceId, flags, budgetId, reserved, payload, extension);

            replySeq = sequence + reserved;

            assert replyAck <= replySeq;
        }

        private void flush(
            long sequence,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            OctetsFW extension)
        {
            assert sequence >= replySeq;

            replySeq = sequence;

            assert replyAck <= replySeq;

            doFlush(receiver, routeId, replyId, replySeq, replyAck, replyMax,
                    traceId, authorization, budgetId, reserved, extension);
        }

        private void end(
            long sequence,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert sequence >= replySeq;

            replySeq = sequence;

            assert replyAck <= replySeq;

            doEnd(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, authorization, extension);
        }

        private void abort(
            long sequence,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert sequence >= replySeq;

            replySeq = sequence;

            assert replyAck <= replySeq;

            doAbort(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, authorization, extension);
        }

        private void reset(
            long acknowledge,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert acknowledge >= initialAck;

            initialAck = acknowledge;

            assert initialAck <= initialSeq;

            doReset(receiver, routeId, initialId, initialSeq, initialAck, initialMax, traceId, authorization, extension);
        }

        private void challenge(
            long acknowledge,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert acknowledge >= initialAck;

            initialAck = acknowledge;

            assert initialAck <= initialSeq;

            doChallenge(receiver, routeId, initialId, initialSeq, initialAck, initialMax, traceId, authorization, extension);
        }

        private void credit(
            long traceId,
            long budgetId,
            int minInitialWindow,
            int minInitialPad,
            int minInitialMax)
        {
            final long newInitialAck = Math.max(initialSeq - minInitialWindow, initialAck);

            if (newInitialAck > initialAck || minInitialMax > initialMax)
            {
                initialAck = newInitialAck;
                assert initialAck <= initialSeq;

                initialMax = minInitialMax;

                doWindow(receiver, routeId, initialId, initialSeq, initialAck, initialMax, traceId, budgetId, minInitialPad);
            }
        }

        private void flush(
            final DirectBuffer buffer,
            final int offset,
            final int limit)
        {
            final DataFW data = dataRO.wrap(buffer, offset, limit);

            final long sequence = data.sequence();
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            client.send(sequence, traceId, flags, budgetId, reserved, payload, extension);
        }
    }

    private final class FlowProxyClient
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private MessageConsumer receiver;

        private FlowProxyServer server;

        private long initialSeq;
        private long initialAck;
        private int initialMax;
        private int initialPad;

        private long replySeq;
        private long replyAck;
        private int replyMax;
        private int replyPad;

        private int replySlot;
        private int replySlotOffset;
        private int replySignals;
        private int remainingSignals;

        FlowProxyClient(
            long routeId)
        {
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.replySlot = NO_SLOT;
        }

        private void correlate(
            FlowProxyServer accept)
        {
            this.server = accept;
        }

        private void onMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onAbort(abort);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onFlush(flush);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onWindow(window);
                break;
            case SignalFW.TYPE_ID:
                final SignalFW signal = signalRO.wrap(buffer, index, index + length);
                onSignal(signal);
                break;
            case ChallengeFW.TYPE_ID:
                final ChallengeFW challenge = challengeRO.wrap(buffer, index, index + length);
                onChallenge(challenge);
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            final long affinity = begin.affinity();
            final OctetsFW extension = begin.extension();

            replySeq = sequence;
            replyAck = acknowledge;

            server.begin(sequence, acknowledge, traceId, authorization, affinity, extension);
        }

        private void onData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final int dataSize = data.sizeof();

            final int replySlotSize = bufferPool.slotCapacity();

            assert acknowledge <= sequence;
            assert sequence >= replySeq;

            replySeq = sequence + data.reserved();

            assert replyAck <= replySeq;

            if (replySlot == NO_SLOT)
            {
                replySlot = bufferPool.acquire(replyId);
            }

            if (replySeq > replyAck + replyMax || replySlot == NO_SLOT)
            {
                doReject(receiver, routeId, replyId, replySeq, replyAck, replyMax);
                server.onRejected(traceId);
            }
            else if (replySlotOffset == 0 && dataSize + Integer.BYTES > replySlotSize)
            {
                assert replySlot != NO_SLOT;
                assert replySlotOffset == 0;

                final int flags = data.flags();
                final long budgetId = data.budgetId();
                final int reserved = data.reserved();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                server.send(sequence, traceId, flags, budgetId, reserved, payload, extension);

                bufferPool.release(replySlot);
                replySlot = NO_SLOT;
            }
            else
            {
                assert replySlot != NO_SLOT;

                final MutableDirectBuffer replyBuf = bufferPool.buffer(replySlot);

                if (replySlotOffset != 0 && replySlotOffset + dataSize > replySlotSize)
                {
                    flush(replyBuf, Integer.BYTES, replySlotOffset);
                    replySlotOffset = 0;
                }

                if (replySlotOffset == 0)
                {
                    final DirectBuffer dataBuf = data.buffer();
                    final int dataOffset = data.offset();
                    final int extensionSize = data.extension().sizeof();

                    replyBuf.putInt(0, extensionSize);
                    replyBuf.putBytes(Integer.BYTES, dataBuf, dataOffset, dataSize);
                    replySlotOffset += Integer.BYTES + dataSize;
                    remainingSignals = maximumSignals;
                }
                else
                {
                    final OctetsFW fragment = data.payload();
                    final int fragmentReserved = data.reserved();
                    final DirectBuffer fragmentBuf = fragment.buffer();
                    final int fragmentOffset = fragment.offset();
                    final int fragmentSize = fragment.sizeof();
                    final int flagsOffset = Integer.BYTES + DataFW.FIELD_OFFSET_FLAGS;
                    final int flags = replyBuf.getInt(flagsOffset);
                    final int newFlags = flags | (data.flags() & ~0x01) | (data.flags() & 0x02);
                    final int reservedOffset = Integer.BYTES + DataFW.FIELD_OFFSET_RESERVED;
                    final int reserved = replyBuf.getInt(reservedOffset);
                    final int newReserved = reserved + fragmentReserved;
                    final int lengthOffset = Integer.BYTES + DataFW.FIELD_OFFSET_LENGTH;
                    final int length = replyBuf.getInt(lengthOffset);
                    final int newLength = length + fragmentSize;
                    final int extensionSize = replyBuf.getInt(0);
                    final int extensionOffset = replySlotOffset - extensionSize;
                    final int newExtensionOffset = replySlotOffset + fragmentSize - extensionSize;

                    replyBuf.putInt(flagsOffset, newFlags);
                    replyBuf.putInt(lengthOffset, newLength);
                    replyBuf.putInt(reservedOffset, newReserved);
                    replyBuf.putBytes(newExtensionOffset, replyBuf, extensionOffset, extensionSize);
                    replyBuf.putBytes(extensionOffset, fragmentBuf, fragmentOffset, fragmentSize);
                    replySlotOffset += fragmentSize;
                    remainingSignals--;
                }

                if (remainingSignals == 0)
                {
                    flush(replyBuf, Integer.BYTES, replySlotOffset);

                    bufferPool.release(replySlot);
                    replySlot = NO_SLOT;
                    replySlotOffset = 0;
                }
                else
                {
                    replySignals++;
                    signaler.signalNow(routeId, initialId, 0);
                }
            }
        }

        private void onEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long traceId = end.traceId();
            final long authorization = end.authorization();
            final OctetsFW extension = end.extension();

            if (replySlot != NO_SLOT)
            {
                assert replySlot != NO_SLOT;

                final DirectBuffer replyBuf = bufferPool.buffer(replySlot);
                flush(replyBuf, Integer.BYTES, replySlotOffset);

                bufferPool.release(replySlot);
                replySlot = NO_SLOT;
                replySlotOffset = 0;
            }

            server.end(sequence, traceId, authorization, extension);
        }

        private void onFlush(
            FlushFW flush)
        {
            final long sequence = flush.sequence();
            final long traceId = flush.traceId();
            final long authorization = flush.authorization();
            final long budgetId = flush.budgetId();
            final int reserved = flush.reserved();
            final OctetsFW extension = flush.extension();

            if (replySlot != NO_SLOT)
            {
                assert replySlot != NO_SLOT;

                final DirectBuffer replyBuf = bufferPool.buffer(replySlot);
                flush(replyBuf, Integer.BYTES, replySlotOffset);

                bufferPool.release(replySlot);
                replySlot = NO_SLOT;
                replySlotOffset = 0;
            }

            server.flush(sequence, traceId, authorization, budgetId, reserved, extension);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            final OctetsFW extension = abort.extension();

            server.abort(sequence, traceId, authorization, extension);
        }

        private void onReset(
            ResetFW reset)
        {
            final long acknowledge = reset.acknowledge();
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            final OctetsFW extension = reset.extension();

            server.reset(acknowledge, traceId, authorization, extension);
        }

        private void onWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert sequence <= initialSeq;
            assert acknowledge >= initialAck;
            assert maximum >= initialMax;

            this.initialAck = acknowledge;
            this.initialMax = maximum;
            this.initialPad = padding;

            assert initialAck <= initialSeq;

            final int initialWindow = initialMax - (int)(initialSeq - initialAck);
            if (initialWindow > 0) // threshold = 0
            {
                final long traceId = window.traceId();
                final long budgetId = window.budgetId();

                server.credit(traceId, budgetId, initialWindow, initialPad, initialMax);
            }
        }

        private void onSignal(
            SignalFW signal)
        {
            replySignals--;

            if (replySignals == 0 && replySlot != NO_SLOT)
            {
                assert replySlot != NO_SLOT;

                final DirectBuffer replyBuf = bufferPool.buffer(replySlot);
                flush(replyBuf, Integer.BYTES, replySlotOffset);

                bufferPool.release(replySlot);
                replySlot = NO_SLOT;
                replySlotOffset = 0;
            }
        }

        private void onChallenge(
            ChallengeFW challenge)
        {
            final long acknowledge = challenge.acknowledge();
            final long traceId = challenge.traceId();
            final long authorization = challenge.authorization();
            final OctetsFW extension = challenge.extension();

            server.challenge(acknowledge, traceId, authorization, extension);
        }

        private void onRejected(
            long traceId)
        {
            doRejected(receiver, routeId, initialId, initialSeq, initialAck, initialMax, traceId);
        }

        @Override
        public String toString()
        {
            return String.format("[%s] routeId=%016x", getClass().getSimpleName(), routeId);
        }

        private void begin(
            long sequence,
            long acknowledge,
            long traceId,
            long authorization,
            long affinity,
            OctetsFW extension)
        {
            assert acknowledge <= sequence;
            assert sequence >= initialSeq;
            assert acknowledge >= initialAck;

            initialSeq = sequence;
            initialAck = acknowledge;

            receiver = newStream(this::onMessage, routeId, initialId, initialSeq, initialAck, initialMax,
                    traceId, authorization, affinity, extension);
        }

        private void send(
            long sequence,
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            assert sequence >= initialSeq;

            initialSeq = sequence;

            doData(receiver, routeId, initialId, initialSeq, initialAck, initialMax,
                    traceId, flags, budgetId, reserved, payload, extension);

            initialSeq = sequence + reserved;

            assert initialAck <= initialSeq;
        }

        private void flush(
            long sequence,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            OctetsFW extension)
        {
            assert sequence >= initialSeq;

            initialSeq = sequence;

            assert initialAck <= initialSeq;

            doFlush(receiver, routeId, initialId, initialSeq, initialAck, initialMax,
                    traceId, authorization, budgetId, reserved, extension);
        }

        private void end(
            long sequence,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert sequence >= initialSeq;

            initialSeq = sequence;

            assert initialAck <= initialSeq;

            doEnd(receiver, routeId, initialId, initialSeq, initialAck, initialMax, traceId, authorization, extension);
        }

        private void abort(
            long sequence,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert sequence >= initialSeq;

            initialSeq = sequence;

            assert initialAck <= initialSeq;

            doAbort(receiver, routeId, initialId, initialSeq, initialAck, initialMax, traceId, authorization, extension);
        }

        private void reset(
            long acknowledge,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert acknowledge >= replyAck;

            replyAck = acknowledge;

            assert replyAck <= replySeq;

            doReset(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, authorization, extension);
        }

        private void challenge(
            long acknowledge,
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            assert acknowledge >= replyAck;

            replyAck = acknowledge;

            assert replyAck <= replySeq;

            doChallenge(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, authorization, extension);
        }

        private void credit(
            long traceId,
            long budgetId,
            int minReplyWindow,
            int minReplyPad,
            int minReplyMax)
        {
            final long newReplyAck = Math.max(replySeq - minReplyWindow, replyAck);
            final int newReplyPad = Math.max(replyPad, minReplyPad);

            replyPad = newReplyPad;

            if (newReplyAck > replyAck || minReplyMax > replyMax)
            {
                replyAck = newReplyAck;
                assert replyAck <= replySeq;

                replyMax = minReplyMax;

                doWindow(receiver, routeId, replyId, replySeq, replyAck, replyMax, traceId, budgetId, newReplyPad);
            }
        }

        private void flush(
            final DirectBuffer buffer,
            final int offset,
            final int limit)
        {
            final DataFW data = dataRO.wrap(buffer, offset, limit);

            final long sequence = data.sequence();
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            server.send(sequence, traceId, flags, budgetId, reserved, payload, extension);
        }
    }

    private MessageConsumer newStream(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long affinity,
        OctetsFW extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(extension)
                .build();

        MessageConsumer receiver =
                streamFactory.newStream(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), sender);

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

        return receiver;
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long authorization,
        long traceId,
        long affinity,
        OctetsFW extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(extension)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        int flags,
        long budgetId,
        int reserved,
        OctetsFW payload,
        OctetsFW extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .flags(flags)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload)
                .extension(extension)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doFlush(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        OctetsFW extension)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .reserved(reserved)
                .extension(extension)
                .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        OctetsFW extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .extension(extension)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        OctetsFW extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .extension(extension)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long sequence,
        final long acknowledge,
        final int maximum,
        final long traceId,
        final long budgetId,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .budgetId(budgetId)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long sequence,
        final long acknowledge,
        final int maximum,
        final long traceId,
        final long authorization,
        final OctetsFW extension)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .sequence(sequence)
               .acknowledge(acknowledge)
               .maximum(maximum)
               .traceId(traceId)
               .authorization(authorization)
               .extension(extension)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doChallenge(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long sequence,
        final long acknowledge,
        final int maximum,
        final long traceId,
        final long authorization,
        final OctetsFW extension)
    {
        final ChallengeFW challenge = challengeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .sequence(sequence)
               .acknowledge(acknowledge)
               .maximum(maximum)
               .traceId(traceId)
               .authorization(authorization)
               .extension(extension)
               .build();

        sender.accept(challenge.typeId(), challenge.buffer(), challenge.offset(), challenge.sizeof());
    }

    private void doReject(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long sequence,
        final long acknowledge,
        final int maximum)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .sequence(sequence)
               .acknowledge(acknowledge)
               .maximum(maximum)
               .traceId(supplyTraceId.getAsLong())
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doRejected(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }
}
