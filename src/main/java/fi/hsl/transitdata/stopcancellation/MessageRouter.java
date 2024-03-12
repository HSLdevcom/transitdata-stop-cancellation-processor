package fi.hsl.transitdata.stopcancellation;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.gtfsrt.FeedMessageFactory;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.TransitdataProperties.*;
import fi.hsl.common.transitdata.TransitdataSchema;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stopcancellation.models.TripUpdateWithId;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MessageRouter implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private final Consumer<byte[]> consumer;
    private final Producer<byte[]> producer;

    private final ThrottledConsumer throttledConsumer;
    private final long futureTripUpdateSendDuration;

    private final StopCancellationProcessor stopCancellationProcessor;

    public MessageRouter(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();

        final Config config = context.getConfig();

        throttledConsumer = new ThrottledConsumer(Executors.newSingleThreadExecutor(runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        }));
        futureTripUpdateSendDuration = config.getDuration("processor.futureTripUpdateSendDuration").toMillis();

        stopCancellationProcessor = new StopCancellationProcessor(ZoneId.of(config.getString("processor.timezone")),
                config.getDuration("processor.tripUpdateCacheDuration"),
                config.getDuration("processor.tripStopCancellationCacheDuration"));
    }

    public void handleMessage(Message received) throws Exception {
        try {
            Optional<TransitdataSchema> maybeSchema = TransitdataSchema.parseFromPulsarMessage(received);
            maybeSchema.ifPresent(schema -> {
                try {
                    if (schema.schema == ProtobufSchema.StopCancellations) {
                        stopCancellationProcessor.updateStopCancellations(InternalMessages.StopCancellations.parseFrom(received.getData()));

                        //Create NO_DATA trip updates for trips that have cancelled stops but are not producing trip updates yet
                        Collection<TripUpdateWithId> tripUpdates = stopCancellationProcessor
                                .getStopCancellationTripUpdates(received.getEventTime() / 1000); //Pulsar timestamp in milliseconds, trip update in seconds

                        //Throttle sending future trip updates to avoid overloading MQTT broker
                        //TODO: consider if there would be another way to avoid overloading MQTT broker, e.g. batching messages
                        throttledConsumer.consumeThrottled(tripUpdates, tripUpdateWithId -> {
                            log.debug("Sending stop cancellation trip update for {}", tripUpdateWithId.id);
                            sendTripUpdate(tripUpdateWithId.id,
                                    tripUpdateWithId.tripUpdate,
                                    received.getEventTime());
                        }, futureTripUpdateSendDuration);
                    } else if (schema.schema == ProtobufSchema.GTFS_TripUpdate) {
                        final GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(received.getData());

                        if (feedMessage.getEntityCount() == 1) {
                            final GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);

                            if (entity.hasTripUpdate()) {
                                final String tripId = received.getKey();
                                final GtfsRealtime.TripUpdate tripUpdate = stopCancellationProcessor.applyStopCancellations(feedMessage.getEntity(0).getTripUpdate());
                                
                                for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : tripUpdate.getStopTimeUpdateList()) {
                                    String assignedStopId = stopTimeUpdate.getStopTimeProperties().getAssignedStopId();
                                    
                                    if (StringUtils.isNotBlank(assignedStopId)) {
                                        log.info("AssignedStopId is set. AssignedStopId={}, StopId={}, StopSequence={}, RouteId={}, DirectionId={}, OperationDay={}, StartTime={}",
                                                assignedStopId, stopTimeUpdate.getStopId(), stopTimeUpdate.getStopSequence(),
                                                tripUpdate.getTrip().getRouteId(), tripUpdate.getTrip().getDirectionId(),
                                                tripUpdate.getTrip().getStartDate(), tripUpdate.getTrip().getStartTime());
                                    } else if ("2015".equals(tripUpdate.getTrip().getRouteId())) {
                                        log.info("Route is 2015. AssignedStopId={}, StopId={}, StopSequence={}, RouteId={}, DirectionId={}, OperationDay={}, StartTime={}",
                                                assignedStopId, stopTimeUpdate.getStopId(), stopTimeUpdate.getStopSequence(),
                                                tripUpdate.getTrip().getRouteId(), tripUpdate.getTrip().getDirectionId(),
                                                tripUpdate.getTrip().getStartDate(), tripUpdate.getTrip().getStartTime());
                                    }
                                }

                                sendTripUpdate(tripId, tripUpdate, received.getEventTime());
                            } else {
                                log.warn("Feed entity {} did not contain a trip update", entity.getId());
                            }
                        } else {
                            log.warn("Feed message had invalid amount of entities: {}, expected 1", feedMessage.getEntityCount());
                        }
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
        } catch (Exception e) {
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
