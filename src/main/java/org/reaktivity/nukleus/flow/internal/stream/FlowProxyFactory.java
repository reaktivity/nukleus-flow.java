/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.concurrent.Signaler.NO_CANCEL_ID;

import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.flow.internal.FlowConfiguration;
import org.reaktivity.nukleus.flow.internal.types.OctetsFW;
import org.reaktivity.nukleus.flow.internal.types.control.RouteFW;
import org.reaktivity.nukleus.flow.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.flow.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.flow.internal.types.stream.ChallengeFW;
import org.reaktivity.nukleus.flow.internal.types.stream.DataFW;
import org.reaktivity.nukleus.flow.internal.types.stream.EndFW;
import org.reaktivity.nukleus.flow.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.flow.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.flow.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.flow.internal.types.stream.SignalFW;
import org.reaktivity.nukleus.flow.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class FlowProxyFactory implements StreamFactory
{
    private final RouteFW routeRO = new RouteFW();

    private final FrameFW frameRO = new FrameFW();

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
    private final SignalFW.Builder signalRW = new SignalFW.Builder();
    private final ChallengeFW.Builder challengeRW = new ChallengeFW.Builder();

    private final RouteManager router;
    private final BufferPool bufferPool;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;

    private final MessageFunction<RouteFW> wrapRoute;
    private final Long2ObjectHashMap<FlowProxyConnect> correlations;

    private final int maximumSignals;

    public FlowProxyFactory(
        FlowConfiguration config,
        RouteManager router,
        BufferPool bufferPool,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId)
    {
        this.router = requireNonNull(router);
        this.bufferPool = requireNonNull(bufferPool);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.wrapRoute = this::wrapRoute;
        this.correlations = new Long2ObjectHashMap<>();
        this.maximumSignals = config.maximumSignals();
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
        final long streamId = begin.streamId();

        MessageConsumer newStream;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newInitialStream(begin, sender);
        }
        else
        {
            newStream = newReplyStream(begin, sender);
        }

        return newStream;
    }

    private MessageConsumer newInitialStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long routeId = begin.routeId();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(routeId, begin.authorization(), filter, wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long initialId = begin.streamId();

            final FlowProxyAccept accept = new FlowProxyAccept(sender, routeId, initialId);
            final FlowProxyConnect connect = new FlowProxyConnect(route.correlationId());

            accept.correlate(connect);
            connect.correlate(accept);

            correlations.put(connect.replyId, connect);

            newStream = accept::onStream;
        }

        return newStream;
    }

    private MessageConsumer newReplyStream(
        final BeginFW begin,
        final MessageConsumer sender)
    {
        final long replyId = begin.streamId();
        final FlowProxyConnect connect = correlations.remove(replyId);

        MessageConsumer newStream = null;
        if (connect != null)
        {
            newStream = connect::onStream;
        }

        return newStream;
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class FlowProxyAccept
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;

        private FlowProxyConnect connect;

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
        private MessageConsumer signaler;
        private int remainingSignals;

        private FlowProxyAccept(
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
            FlowProxyConnect connect)
        {
            this.connect = connect;
            this.signaler = router.supplyReceiver(initialId);
        }

        private void onStream(
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
            default:
                final FrameFW frame = frameRO.wrap(buffer, index, index + length);
                final long sequence = frame.sequence();
                final long acknowledge = frame.acknowledge();
                final int maximum = frame.maximum();
                doReject(receiver, routeId, initialId, sequence, acknowledge, maximum);
                break;
            }
        }

        private void onThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
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
            default:
                // ignore
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

            connect.begin(sequence, acknowledge, traceId, authorization, affinity, extension);
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
                connect.onRejected(traceId);
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

                connect.send(initialSeq, traceId, flags, budgetId, reserved, payload, extension);

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
                    doSignal(signaler, routeId, replyId, replySeq, replyAck, replyMax, traceId, 0);
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

            connect.flush(sequence, traceId, authorization, budgetId, reserved, extension);
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

            connect.end(sequence, traceId, authorization, extension);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            final OctetsFW extension = abort.extension();

            connect.abort(sequence, traceId, authorization, extension);
        }

        private void onReset(
            ResetFW reset)
        {
            final long acknowledge = reset.acknowledge();
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            final OctetsFW extension = reset.extension();

            connect.reset(acknowledge, traceId, authorization, extension);
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

                connect.credit(traceId, budgetId, replyWindow, replyPad, replyMax);
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

            connect.challenge(acknowledge, traceId, authorization, extension);
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

            router.setThrottle(replyId, this::onThrottle);
            doBegin(receiver, routeId, replyId, replySeq, replyAck, replyMax, authorization, traceId, affinity, extension);
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

            connect.send(sequence, traceId, flags, budgetId, reserved, payload, extension);
        }
    }

    private final class FlowProxyConnect
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;

        private FlowProxyAccept accept;

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
        private MessageConsumer signaler;
        private int remainingSignals;

        FlowProxyConnect(
            long routeId)
        {
            this.routeId = routeId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);
            this.replySlot = NO_SLOT;
        }

        private void correlate(
            FlowProxyAccept accept)
        {
            this.accept = accept;
            this.signaler = router.supplyReceiver(accept.initialId);
        }

        private void onStream(
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
            default:
                final FrameFW frame = frameRO.wrap(buffer, index, index + length);
                final long sequence = frame.sequence();
                final long acknowledge = frame.acknowledge();
                final int maximum = frame.maximum();
                doReject(receiver, routeId, initialId, sequence, acknowledge, maximum);
                break;
            }
        }

        private void onThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
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
            default:
                // ignore
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

            accept.begin(sequence, acknowledge, traceId, authorization, affinity, extension);
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
                accept.onRejected(traceId);
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

                accept.send(sequence, traceId, flags, budgetId, reserved, payload, extension);

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
                    doSignal(signaler, routeId, initialId, initialSeq, initialAck, initialMax, traceId, 0);
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

            accept.end(sequence, traceId, authorization, extension);
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

            accept.flush(sequence, traceId, authorization, budgetId, reserved, extension);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            final OctetsFW extension = abort.extension();

            accept.abort(sequence, traceId, authorization, extension);
        }

        private void onReset(
            ResetFW reset)
        {
            final long acknowledge = reset.acknowledge();
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            final OctetsFW extension = reset.extension();

            accept.reset(acknowledge, traceId, authorization, extension);
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

                accept.credit(traceId, budgetId, initialWindow, initialPad, initialMax);
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

            accept.challenge(acknowledge, traceId, authorization, extension);
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

            doBegin(receiver, routeId, initialId, initialSeq, initialAck, initialMax,
                    authorization, traceId, affinity, extension);
            router.setThrottle(initialId, this::onThrottle);
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

            accept.send(sequence, traceId, flags, budgetId, reserved, payload, extension);
        }
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

    private void doSignal(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        int signalId)
    {
        final SignalFW signal = signalRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .cancelId(NO_CANCEL_ID)
                .signalId(signalId)
                .build();

        receiver.accept(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
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
