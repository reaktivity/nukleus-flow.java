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
import org.reaktivity.nukleus.flow.internal.types.stream.DataFW;
import org.reaktivity.nukleus.flow.internal.types.stream.EndFW;
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

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final SignalFW signalRO = new SignalFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final SignalFW.Builder signalRW = new SignalFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

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

        private int initialBudget;
        private int replyBudget;
        private int replyPadding;

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
            default:
                doReject(receiver, routeId, initialId);
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
            default:
                // ignore
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            final long affinity = begin.affinity();
            final OctetsFW extension = begin.extension();

            connect.begin(traceId, authorization, affinity, extension);
        }

        private void onData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int dataSize = data.sizeof();

            final int initialSlotSize = bufferPool.slotCapacity();

            initialBudget -= data.reserved();

            if (initialSlot == NO_SLOT)
            {
                initialSlot = bufferPool.acquire(replyId);
            }

            if (initialBudget < 0 || initialSlot == NO_SLOT)
            {
                doReject(receiver, routeId, initialId);
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

                connect.send(traceId, flags, budgetId, reserved, payload, extension);

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
                    final DirectBuffer fragmentBuf = fragment.buffer();
                    final int fragmentOffset = fragment.offset();
                    final int fragmentSize = fragment.sizeof();
                    final int flagsOffset = Integer.BYTES + DataFW.FIELD_OFFSET_FLAGS;
                    final int flags = initialBuf.getInt(flagsOffset);
                    final int newFlags = flags | (data.flags() & ~0x01) | (data.flags() & 0x02);
                    final int lengthOffset = Integer.BYTES + DataFW.FIELD_OFFSET_LENGTH;
                    final int length = initialBuf.getInt(lengthOffset);
                    final int newLength = length + fragmentSize;
                    final int extensionSize = initialBuf.getInt(0);
                    final int extensionOffset = initialSlotOffset - extensionSize;
                    final int newExtensionOffset = initialSlotOffset + fragmentSize - extensionSize;

                    initialBuf.putInt(flagsOffset, newFlags);
                    initialBuf.putInt(lengthOffset, newLength);
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
                    doSignal(signaler, routeId, replyId, traceId, 0);
                }
            }
        }

        private void onEnd(
            EndFW end)
        {
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

            connect.end(traceId, authorization, extension);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            final OctetsFW extension = abort.extension();

            connect.abort(traceId, authorization, extension);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            final OctetsFW extension = reset.extension();

            connect.reset(traceId, authorization, extension);
        }

        private void onWindow(
            WindowFW window)
        {
            final int credit = window.credit();
            final int padding = window.padding();

            this.replyBudget += credit;
            this.replyPadding = padding;

            if (credit > 0 && replyBudget > 0) // threshold = 0
            {
                final long traceId = window.traceId();
                connect.credit(replyBudget, replyPadding, traceId);
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

        private void onRejected(
            long traceId)
        {
            doRejected(receiver, routeId, replyId, traceId);
        }

        private void begin(
            long traceId,
            long authorization,
            long affinity,
            OctetsFW extension)
        {
            router.setThrottle(replyId, this::onThrottle);
            doBegin(receiver, routeId, replyId, authorization, traceId, affinity, extension);
        }

        private void send(
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            replyBudget -= reserved;
            doData(receiver, routeId, replyId, traceId, flags, budgetId, reserved, payload, extension);
        }

        private void end(
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            doEnd(receiver, routeId, replyId, traceId, authorization, extension);
        }

        private void abort(
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            doAbort(receiver, routeId, replyId, traceId, authorization, extension);
        }

        private void reset(
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            doReset(receiver, routeId, initialId, traceId, authorization, extension);
        }

        private void credit(
            long traceId,
            long budgetId,
            int maxInitialBudget,
            int minInitialPadding)
        {
            final int initialCredit = maxInitialBudget - initialBudget;
            if (initialCredit > 0)
            {
                doWindow(receiver, routeId, initialId, traceId, budgetId, initialCredit, minInitialPadding);
                initialBudget = maxInitialBudget;
            }
        }

        private void flush(
            final DirectBuffer buffer,
            final int offset,
            final int limit)
        {
            final DataFW data = dataRO.wrap(buffer, offset, limit);

            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            connect.send(traceId, flags, budgetId, reserved, payload, extension);
        }
    }

    private final class FlowProxyConnect
    {
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;

        private FlowProxyAccept accept;

        private int initialBudget;
        private int initialPadding;

        private int replyBudget;
        private int replyPadding;
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
            default:
                doReject(receiver, routeId, initialId);
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
            default:
                // ignore
                break;
            }
        }

        private void onBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            final long affinity = begin.affinity();
            final OctetsFW extension = begin.extension();

            accept.begin(traceId, authorization, affinity, extension);
        }

        private void onData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int dataSize = data.sizeof();

            final int replySlotSize = bufferPool.slotCapacity();

            replyBudget -= data.reserved();

            if (replySlot == NO_SLOT)
            {
                replySlot = bufferPool.acquire(replyId);
            }

            if (replyBudget < 0 || replySlot == NO_SLOT)
            {
                doReject(receiver, routeId, initialId);
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

                accept.send(traceId, flags, budgetId, reserved, payload, extension);

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
                    final DirectBuffer fragmentBuf = fragment.buffer();
                    final int fragmentOffset = fragment.offset();
                    final int fragmentSize = fragment.sizeof();
                    final int flagsOffset = Integer.BYTES + DataFW.FIELD_OFFSET_FLAGS;
                    final int flags = replyBuf.getInt(flagsOffset);
                    final int newFlags = flags | (data.flags() & ~0x01) | (data.flags() & 0x02);
                    final int lengthOffset = Integer.BYTES + DataFW.FIELD_OFFSET_LENGTH;
                    final int length = replyBuf.getInt(lengthOffset);
                    final int newLength = length + fragmentSize;
                    final int extensionSize = replyBuf.getInt(0);
                    final int extensionOffset = replySlotOffset - extensionSize;
                    final int newExtensionOffset = replySlotOffset + fragmentSize - extensionSize;

                    replyBuf.putInt(flagsOffset, newFlags);
                    replyBuf.putInt(lengthOffset, newLength);
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
                    doSignal(signaler, routeId, initialId, traceId, 0);
                }
            }
        }

        private void onEnd(
            EndFW end)
        {
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

            accept.end(traceId, authorization, extension);
        }

        private void onAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            final OctetsFW extension = abort.extension();

            accept.abort(traceId, authorization, extension);
        }

        private void onReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            final OctetsFW extension = reset.extension();

            accept.reset(traceId, authorization, extension);
        }

        private void onWindow(
            WindowFW window)
        {
            final int credit = window.credit();
            final int padding = window.padding();

            this.initialBudget += credit;
            this.initialPadding = padding;

            if (credit > 0 && initialBudget > 0) // threshold = 0
            {
                final long traceId = window.traceId();
                final long budgetId = window.budgetId();

                accept.credit(traceId, budgetId, initialBudget, initialPadding);
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

        private void onRejected(
            long traceId)
        {
            doRejected(receiver, routeId, initialId, traceId);
        }

        @Override
        public String toString()
        {
            return String.format("[%s] routeId=%016x", getClass().getSimpleName(), routeId);
        }

        private void begin(
            long traceId,
            long authorization,
            long affinity,
            OctetsFW extension)
        {
            doBegin(receiver, routeId, initialId, authorization, traceId, affinity, extension);
            router.setThrottle(initialId, this::onThrottle);
        }

        private void send(
            long traceId,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload,
            OctetsFW extension)
        {
            initialBudget -= reserved;
            doData(receiver, routeId, initialId, traceId, flags, budgetId, reserved, payload, extension);
        }

        private void end(
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            doEnd(receiver, routeId, initialId, traceId, authorization, extension);
        }

        private void abort(
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            doAbort(receiver, routeId, initialId, traceId, authorization, extension);
        }

        private void reset(
            long traceId,
            long authorization,
            OctetsFW extension)
        {
            doReset(receiver, routeId, replyId, traceId, authorization, extension);
        }

        private void credit(
            int minReplyBudget,
            int minReplyPadding,
            long traceId)
        {
            final int newReplyBudget = Math.max(replyBudget, minReplyBudget);
            final int newReplyPadding = Math.max(replyPadding, minReplyPadding);

            replyPadding = newReplyPadding;

            final int replyCredit = newReplyBudget - replyBudget;
            if (replyCredit > 0)
            {
                doWindow(receiver, routeId, replyId, traceId, 0L, replyCredit, newReplyPadding);
                replyBudget = newReplyBudget;
            }
        }

        private void flush(
            final DirectBuffer buffer,
            final int offset,
            final int limit)
        {
            final DataFW data = dataRO.wrap(buffer, offset, limit);

            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            accept.send(traceId, flags, budgetId, reserved, payload, extension);
        }
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long authorization,
        long traceId,
        long affinity,
        OctetsFW extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
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
        long traceId,
        int signalId)
    {
        final SignalFW signal = signalRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .cancelId(NO_CANCEL_ID)
                .signalId(signalId)
                .build();

        receiver.accept(signal.typeId(), signal.buffer(), signal.offset(), signal.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        OctetsFW extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
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
        long traceId,
        long authorization,
        OctetsFW extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
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
        final long traceId,
        final long budgetId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final long authorization,
        final OctetsFW extension)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .traceId(traceId)
               .authorization(authorization)
               .extension(extension)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doReject(
        final MessageConsumer sender,
        final long routeId,
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .traceId(supplyTraceId.getAsLong())
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doRejected(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }
}
