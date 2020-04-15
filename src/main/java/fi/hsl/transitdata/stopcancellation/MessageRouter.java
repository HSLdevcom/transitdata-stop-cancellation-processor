package fi.hsl.transitdata.stopcancellation;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MessageRouter implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    private StopCancellationProcessor stopCancellationProcessor = new StopCancellationProcessor();

    public MessageRouter(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();
    }

    public void handleMessage(Message received) throws Exception {
        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(received);
            maybeSchema.ifPresent(schema -> {
                try {
                    if (schema.schema == ProtobufSchema.StopCancellation) {
                        stopCancellationProcessor.updateStopCancellations(InternalMessages.StopCancellations.parseFrom(received.getData()));
                    } else if (schema.schema == ProtobufSchema.GTFS_TripUpdate) {
                        final String tripId = received.getKey();
                        final GtfsRealtime.TripUpdate tripUpdate = stopCancellationProcessor.applyStopCancellations(GtfsRealtime.TripUpdate.parseFrom(received.getData()));

                        sendTripUpdate(tripId, tripUpdate, received.getEventTime());
                    } else {
                        log.warn("Received message with unknown schema ({}), ignoring", schema);
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Failed to parse protobuf with schema {}", schema.schema, e);
                }
            });

            consumer.acknowledgeAsync(received)
                    .exceptionally(throwable -> {
                        log.error("Failed to ack Pulsar message", throwable);
                        return null;
                    })
                    .thenRun(() -> {});
        }
        catch (Exception e) {
            log.error("Exception while handling message", e);
        }
    }

    private void sendTripUpdate(final String tripId, final GtfsRealtime.TripUpdate tripUpdate, final long pulsarEventTimestamp) {
        GtfsRealtime.FeedMessage feedMessage = FeedMessageFactory.createDifferentialFeedMessage(tripId, tripUpdate, tripUpdate.getTimestamp());
        producer.newMessage()
                .key(tripId)
                .eventTime(pulsarEventTimestamp)
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, TransitdataProperties.ProtobufSchema.GTFS_TripUpdate.toString())
                .value(feedMessage.toByteArray())
                .sendAsync()
                .thenRun(() -> log.debug("Sending TripUpdate for {} with stop cancellations ({} StopTimeUpdates, status {})",
                        tripId, tripUpdate.getStopTimeUpdateCount(), tripUpdate.getTrip().getScheduleRelationship()));

    }
}
