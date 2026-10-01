import java.util.*;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;

/** Executed reference: Apache 4.3.1 FetchSessionHandler, not a reimplementation. */
public final class FetchSessionOracle {
    static final Uuid FIRST = new Uuid(0x0101010101010101L, 0x0101010101010101L);
    static final Uuid SECOND = new Uuid(0x0202020202020202L, 0x0202020202020202L);
    static LinkedHashMap<TopicPartition, FetchRequest.PartitionData> partitions(Uuid id, boolean changed, boolean removed) {
        var result = new LinkedHashMap<TopicPartition, FetchRequest.PartitionData>();
        for (int p=0; p<128; p++) {
            if (removed && p==17) continue;
            result.put(new TopicPartition("t", p), new FetchRequest.PartitionData(id,
                changed && p==63 ? 1L : 0L, -1L, 1048576, Optional.of(0), Optional.empty()));
        }
        return result;
    }
    static FetchSessionHandler.FetchRequestData request(FetchSessionHandler handler,
            LinkedHashMap<TopicPartition, FetchRequest.PartitionData> desired) {
        var builder = handler.newBuilder();
        desired.forEach(builder::add);
        return builder.build();
    }
    static FetchResponse response(int session, short error, Uuid id, Collection<TopicPartition> partitions) {
        var responses = new ArrayList<FetchResponseData.FetchableTopicResponse>();
        if (!partitions.isEmpty()) {
            var rows = new ArrayList<FetchResponseData.PartitionData>();
            for (var partition: partitions) rows.add(new FetchResponseData.PartitionData()
                .setPartitionIndex(partition.partition()).setErrorCode((short)0)
                .setHighWatermark(0).setLastStableOffset(0).setLogStartOffset(0));
            responses.add(new FetchResponseData.FetchableTopicResponse().setTopic("t").setTopicId(id).setPartitions(rows));
        }
        return FetchResponse.of(new FetchResponseData().setSessionId(session).setErrorCode(error).setResponses(responses));
    }
    static void row(short version, String step, FetchSessionHandler.FetchRequestData data) {
        var wire = FetchRequest.Builder.forConsumer(version, 500, 1, data.toSend())
            .metadata(data.metadata()).removed(data.toForget()).replaced(data.toReplace())
            .setMaxBytes(52428800).rackId("").build(version);
        int bytes = MessageUtil.toByteBufferAccessor(wire.data(), version).buffer().remaining();
        System.out.println("{\"version\":"+version+",\"step\":\""+step+"\",\"session_id\":"+data.metadata().sessionId()
            +",\"epoch\":"+data.metadata().epoch()+",\"changed\":"+data.toSend().size()
            +",\"forgotten\":"+(data.toForget().size()+data.toReplace().size())
            +",\"cached_partitions\":"+data.sessionPartitions().size()+",\"request_bytes\":"+bytes+"}");
    }
    static void check(boolean value) { if (!value) throw new AssertionError("Apache handler rejected valid response"); }
    static void recovery(short version, String fault) {
        var handler = new FetchSessionHandler(new LogContext(), 1);
        var desired = partitions(FIRST, false, false);
        request(handler, desired);
        boolean full = fault.equals("missing-full") || fault.equals("throttled-full");
        if (!full) {
            check(handler.handleResponse(response(91, (short)0, FIRST, desired.keySet()), version));
            request(handler, desired);
        }
        if (fault.equals("terminal-close")) {
            handler.notifyClose();
        } else {
            FetchResponse bad = switch (fault) {
                case "missing-full" -> response(91, (short)0, FIRST, List.of());
                case "throttled-full" -> response(0, (short)0, FIRST, List.of());
                case "extra-incremental" -> response(91, (short)0, FIRST, List.of(new TopicPartition("t",128)));
                case "unknown-id" -> response(91, (short)0, SECOND, List.of(new TopicPartition("t",0)));
                case "topic-id-error" -> response(0, Errors.FETCH_SESSION_TOPIC_ID_ERROR.code(), FIRST, List.of());
                default -> throw new AssertionError(fault);
            };
            if (fault.equals("throttled-full")) bad.data().setThrottleTimeMs(80);
            check(!handler.handleResponse(bad, version));
        }
        row(version, fault, request(handler, desired));
    }
    public static void main(String[] args) {
        for (short version: new short[]{7,8,11,12,13,17}) {
            var handler = new FetchSessionHandler(new LogContext(), 1);
            var desired = partitions(FIRST, false, false);
            row(version, "initial", request(handler, desired));
            check(handler.handleResponse(response(91, (short)0, FIRST, desired.keySet()), version));
            row(version, "unchanged", request(handler, desired));
            check(handler.handleResponse(response(91, (short)0, FIRST, List.of()), version));
            desired = partitions(FIRST, true, false);
            row(version, "changed-offset", request(handler, desired));
            check(handler.handleResponse(response(91, (short)0, FIRST, List.of(new TopicPartition("t",63))), version));
            desired = partitions(FIRST, true, true);
            row(version, "removed", request(handler, desired));
            check(handler.handleResponse(response(91, (short)0, FIRST, List.of()), version));
            desired = partitions(SECOND, true, true);
            row(version, "replaced-id", request(handler, desired));
            check(handler.handleResponse(response(91, (short)0, SECOND, desired.keySet()), version));
            check(!handler.handleResponse(response(0, Errors.INVALID_FETCH_SESSION_EPOCH.code(), SECOND, List.of()), version));
            row(version, "invalid-epoch-full", request(handler, desired));
            check(handler.handleResponse(response(91, (short)0, SECOND, desired.keySet()), version));
            check(!handler.handleResponse(response(0, Errors.FETCH_SESSION_ID_NOT_FOUND.code(), SECOND, List.of()), version));
            row(version, "not-found-initial", request(handler, desired));
            check(handler.handleResponse(response(91, (short)0, SECOND, desired.keySet()), version));
            handler.handleError(new java.io.IOException("isolated reference connection loss"));
            row(version, "connection-error-full", request(handler, desired));
        }
        check(new FetchMetadata(91, Integer.MAX_VALUE).nextIncremental().epoch()==1);
        for (short version: new short[]{7,8,11,12,13,17}) {
            for (String fault: List.of("missing-full", "throttled-full", "extra-incremental", "topic-id-error", "terminal-close"))
                recovery(version, fault);
            if (version>=13) recovery(version, "unknown-id");
        }
    }
}
