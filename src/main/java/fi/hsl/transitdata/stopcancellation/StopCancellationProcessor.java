package fi.hsl.transitdata.stopcancellation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stopcancellation.models.TripUpdateWithId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fi.hsl.transitdata.stopcancellation.TripInfoUtils.*;

public class StopCancellationProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StopCancellationProcessor.class);

    private Map<String, List<InternalMessages.StopCancellations.StopCancellation>> stopCancellationsByStopId;
    private Map<String, InternalMessages.JourneyPattern> journeyPatternById;
    private Map<TripIdentifier, String> journeyPatternIdByTripIdentifier;

    //Cache of trips that have produced trip updates
    private final Cache<TripIdentifier, Boolean> tripsWithTripUpdates = Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(10)).build();

    //Cache of trips that we have sent trip updates with cancelled stops
    private final Cache<TripIdentifier, String> tripsWithCancellations = Caffeine.newBuilder().expireAfterWrite(Duration.ofDays(3)).build(); //TODO: duration should be configurable

    public void updateStopCancellations(InternalMessages.StopCancellations stopCancellations) {
        this.stopCancellationsByStopId = stopCancellations.getStopCancellationsList().stream().collect(Collectors.groupingBy(InternalMessages.StopCancellations.StopCancellation::getStopId));
        this.journeyPatternById = stopCancellations.getAffectedJourneyPatternsList().stream().collect(Collectors.toMap(InternalMessages.JourneyPattern::getJourneyPatternId, Function.identity()));

        this.journeyPatternIdByTripIdentifier = new HashMap<>();
        journeyPatternById.values().forEach(journeyPattern -> {
            journeyPattern.getTripsList().forEach(tripInfo -> {
                journeyPatternIdByTripIdentifier.put(TripIdentifier.fromTripInfo(tripInfo), journeyPattern.getJourneyPatternId());
            });
        });
    }

    /**
     * Creates trip updates for trips that had stop cancellations that were later cancelled (cancellation-of-cancellation)
     * @param timestamp Timestamp used in trip update
     * @return List of trip updates
     */
    private Collection<TripUpdateWithId> getTripUpdatesForCancellationsOfCancellations(final long timestamp) {
        return tripsWithCancellations.asMap().entrySet().stream()
                .filter(tripIdentifierAndId -> !journeyPatternIdByTripIdentifier.containsKey(tripIdentifierAndId.getKey())) //Trip does not have active cancellations
                .filter(tripIdentifierAndId -> Boolean.FALSE.equals(tripsWithTripUpdates.getIfPresent(tripIdentifierAndId.getKey()))) //Trip has not sent trip updates
                .map(tripIdentifierAndId -> {
                    //Create trip update that cancels cancellation
                    GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                            .setTimestamp(timestamp)
                            .setTrip(tripIdentifierAndId.getKey().toGtfsTripDescriptor())
                            .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                                    .setStopSequence(1)
                                    .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(0))
                                    .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(0)))
                            .build();

                    return new TripUpdateWithId(tripIdentifierAndId.getValue(), tripUpdate);
                })
                .collect(Collectors.toList());
    }

    /**
     * Creates trip updates for future trips that have stop cancellations
     * @param timestamp Timestamp used in trip update
     * @return List of trip updates
     */
    private Collection<TripUpdateWithId> getTripUpdatesForFutureStopCancellations(final long timestamp) {
        return journeyPatternById.values().stream()
                .map(journeyPattern -> {
                    List<TripUpdateWithId> tripUpdates = new ArrayList<>();

                    final List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = journeyPattern.getStopsList().stream()
                            .sorted(Comparator.comparingInt(InternalMessages.JourneyPattern.Stop::getStopSequence))
                            .map(stop -> {
                                final String stopId = stop.getStopId();

                                GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder().setStopId(stopId);

                                List<InternalMessages.StopCancellations.StopCancellation> stopCancellations = stopCancellationsByStopId.getOrDefault(stopId, Collections.emptyList());
                                if (stopCancellations.stream().anyMatch(stopCancellation -> stopCancellation.getAffectedJourneyPatternIdsList().contains(journeyPattern.getJourneyPatternId()))) {
                                    stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
                                } else {
                                    stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA);
                                }

                                return stopTimeUpdateBuilder.build();
                            }).collect(Collectors.toList());

                    for (InternalMessages.TripInfo tripInfo : journeyPattern.getTripsList()) {
                        final TripIdentifier tripIdentifier = TripIdentifier.fromTripInfo(tripInfo);

                        if (Boolean.TRUE.equals(tripsWithTripUpdates.getIfPresent(tripIdentifier))) {
                            //Trip had already published trip update, no need to create trip update with NO_DATA
                            continue;
                        }

                        final GtfsRealtime.TripDescriptor tripDescriptor = tripIdentifier.toGtfsTripDescriptor();

                        final String id = RouteIdUtils.isTrainRoute(tripInfo.getRouteId()) ?
                                getTrainEntityId(tripDescriptor) :
                                tripInfo.getTripId();

                        //Add cancellation to cache so that it can be cancelled later (cancellation-of-cancellation)
                        tripsWithCancellations.put(tripIdentifier, id);

                        final GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                                .setTrip(tripDescriptor)
                                .setTimestamp(timestamp)
                                .addAllStopTimeUpdate(stopTimeUpdates)
                                .build();

                        tripUpdates.add(new TripUpdateWithId(id, tripUpdate));
                    }

                    return tripUpdates;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Builds trip updates for future trips that have stop cancellations and trips that previously had stop cancellations that were later cancelled (cancellation-of-cancellation)
     * @param timestamp Timestamp (in seconds) to be used in trip updates
     * @return List of trip updates
     */
    public Collection<TripUpdateWithId> getStopCancellationTripUpdates(final long timestamp) {
        Collection<TripUpdateWithId> tripUpdatesWithStopCancellations = getTripUpdatesForFutureStopCancellations(timestamp);

        Collection<TripUpdateWithId> cancellationsOfCancellations = getTripUpdatesForCancellationsOfCancellations(timestamp);

        List<TripUpdateWithId> tripUpdates = new ArrayList<>();
        tripUpdates.addAll(tripUpdatesWithStopCancellations);
        tripUpdates.addAll(cancellationsOfCancellations);

        LOG.info("Created {} trip updates for future stop cancellations and {} trip updates for cancellations of stop cancellations",
                tripUpdatesWithStopCancellations.size(),
                cancellationsOfCancellations.size());

        return tripUpdates;
    }

    /**
     * Applies stop cancellations to the trip update
     * @param tripUpdate Trip update
     * @return Trip update with stop cancellations applied
     */
    public GtfsRealtime.TripUpdate applyStopCancellations(GtfsRealtime.TripUpdate tripUpdate) {
        //Keep track of trips that have produced trip updates (to avoid creating NO_DATA trip updates for them)
        final TripIdentifier tripIdentifier = TripIdentifier.fromGtfsTripDescriptor(tripUpdate.getTrip());

        tripsWithTripUpdates.put(tripIdentifier, true);

        if (tripUpdate.getStopTimeUpdateCount() == 0) {
            //No stop time updates, no stops to cancel
            LOG.debug("Trip {} had no stop time updates, cannot apply stop cancellations", tripIdentifier);
            return tripUpdate;
        }

        final String journeyPatternId = journeyPatternIdByTripIdentifier.get(tripIdentifier);
        if (journeyPatternId == null) {
            //If journeyPatternId is null, the trip was not affected by any stop cancellations
            return tripUpdate;
        }

        final Map<String, GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream()
                .collect(Collectors.toMap(GtfsRealtime.TripUpdate.StopTimeUpdate::getStopId, Function.identity()));

        final InternalMessages.JourneyPattern journeyPattern = journeyPatternById.get(journeyPatternId);

        final List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdateList = journeyPattern.getStopsList().stream()
                .sorted(Comparator.comparingInt(InternalMessages.JourneyPattern.Stop::getStopSequence))
                .map(stop -> {
                    final GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate = stopTimeUpdates.containsKey(stop.getStopId()) ?
                            stopTimeUpdates.get(stop.getStopId()) :
                            GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder().setStopId(stop.getStopId()).setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA).build();

                    final boolean stopCancelled = stopCancellationsByStopId.getOrDefault(stop.getStopId(), Collections.emptyList()).stream()
                            .anyMatch(stopCancellation -> stopCancellation.getAffectedJourneyPatternIdsList().contains(journeyPatternId));

                    if (stopCancelled) {
                        LOG.debug("Cancelled stop {} for trip {}", stop.getStopId(), tripIdentifier);
                        return stopTimeUpdate.toBuilder().setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED).build();
                    } else {
                        return stopTimeUpdate;
                    }
                }).collect(Collectors.toList());

        //Replace original stop time updates with ones where stop cancellations have been applied
        return tripUpdate.toBuilder().clearStopTimeUpdate().addAllStopTimeUpdate(stopTimeUpdateList).build();
    }

    private static class TripIdentifier {
        public final String routeId;
        public final String operatingDate;
        public final String startTime;
        public final int directionId; //Direction ID in GTFS format, 0 or 1

        public TripIdentifier(String routeId, String operatingDate, String startTime, int directionId) {
            this.routeId = routeId;
            this.operatingDate = operatingDate;
            this.startTime = startTime;
            this.directionId = directionId;
        }

        public static TripIdentifier fromTripInfo(final InternalMessages.TripInfo tripInfo) {
            return new TripIdentifier(RouteIdUtils.normalizeRouteId(tripInfo.getRouteId()), tripInfo.getOperatingDay(), tripInfo.getStartTime(), PubtransFactory.joreDirectionToGtfsDirection(tripInfo.getDirectionId()));
        }

        public static TripIdentifier fromGtfsTripDescriptor(final GtfsRealtime.TripDescriptor tripDescriptor) {
            return new TripIdentifier(tripDescriptor.getRouteId(), tripDescriptor.getStartDate(), tripDescriptor.getStartTime(), tripDescriptor.getDirectionId());
        }

        public GtfsRealtime.TripDescriptor toGtfsTripDescriptor() {
            return GtfsRealtime.TripDescriptor.newBuilder().setRouteId(routeId).setStartDate(operatingDate).setStartTime(startTime).setDirectionId(directionId).build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TripIdentifier that = (TripIdentifier) o;
            return directionId == that.directionId &&
                    Objects.equals(routeId, that.routeId) &&
                    Objects.equals(operatingDate, that.operatingDate) &&
                    Objects.equals(startTime, that.startTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(routeId, operatingDate, startTime, directionId);
        }

        @Override
        public String toString() {
            return String.join(" / ", routeId, operatingDate, startTime, String.valueOf(directionId));
        }
    }
}
