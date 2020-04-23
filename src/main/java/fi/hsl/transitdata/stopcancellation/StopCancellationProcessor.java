package fi.hsl.transitdata.stopcancellation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fi.hsl.transitdata.stopcancellation.TripInfoUtils.*;

public class StopCancellationProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StopCancellationProcessor.class);

    private Map<String, List<InternalMessages.StopCancellations.StopCancellation>> stopCancellationsByStopId;
    private Map<String, InternalMessages.JourneyPattern> journeyPatternById;

    //Cache of trips that have produced trip updates
    private Cache<GtfsRealtime.TripDescriptor, Boolean> tripsWithTripUpdates = Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(10)).build();

    public void updateStopCancellations(InternalMessages.StopCancellations stopCancellations) {
        this.stopCancellationsByStopId = stopCancellations.getStopCancellationsList().stream().collect(Collectors.groupingBy(InternalMessages.StopCancellations.StopCancellation::getStopId));
        this.journeyPatternById = stopCancellations.getAffectedJourneyPatternsList().stream().collect(Collectors.toMap(InternalMessages.JourneyPattern::getJourneyPatternId, Function.identity()));
    }

    /**
     * Builds trip updates for trips that have cancelled stops but have not produced trip updates recently
     * @param timestamp Timestamp (in seconds) to be used in trip updates
     * @return List of trip updates
     */
    public Collection<TripUpdateWithId> getStopCancellationTripUpdates(final long timestamp) {
        return journeyPatternById.values().stream().map(journeyPattern -> {
            List<TripUpdateWithId> tripUpdates = new ArrayList<>();

            List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = journeyPattern.getStopsList().stream().map(stop -> {
                final String stopId = stop.getStopId();

                GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stopTimeUpdateBuilder = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder().setStopId(stopId);

                List<InternalMessages.StopCancellations.StopCancellation> stopCancellations = stopCancellationsByStopId.get(stopId);
                if (stopCancellations != null &&
                        stopCancellations.stream().anyMatch(stopCancellation -> stopCancellation.getAffectedJourneyPatternIdsList().contains(journeyPattern.getJourneyPatternId()))) {
                    stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);
                } else {
                    stopTimeUpdateBuilder.setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA);
                }

                return stopTimeUpdateBuilder.build();
            }).collect(Collectors.toList());

            for (InternalMessages.TripInfo tripInfo : journeyPattern.getTripsList()) {
                final GtfsRealtime.TripDescriptor tripDescriptor = toTripDescriptor(tripInfo);
                if (Boolean.TRUE.equals(tripsWithTripUpdates.getIfPresent(tripDescriptor))) {
                    //Trip had already published trip update, no need to create trip update with NO_DATA
                    continue;
                }

                final String id = RouteIdUtils.isTrainRoute(tripInfo.getRouteId()) ?
                        getTrainEntityId(tripDescriptor) :
                        tripInfo.getTripId();

                final GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                        .setTrip(tripDescriptor)
                        .setTimestamp(timestamp)
                        .addAllStopTimeUpdate(stopTimeUpdates)
                        .build();

                tripUpdates.add(new TripUpdateWithId(id, tripUpdate));
            }

            return tripUpdates;
        }).flatMap(List::stream).collect(Collectors.toList());
    }


    private boolean affectsStopTimeUpdate(InternalMessages.StopCancellations.StopCancellation stopCancellation, GtfsRealtime.TripDescriptor trip, GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate) {
        return stopTimeUpdate.getDeparture().getTime() >= stopCancellation.getValidFromUtcMs() &&
                stopTimeUpdate.getArrival().getTime() <= stopCancellation.getValidToUtcMs() &&
                stopCancellation.getAffectedJourneyPatternIdsList().stream()
                        .map(journeyPatternById::get)
                        .filter(Objects::nonNull)
                        .flatMap(journeyPattern -> journeyPattern.getTripsList().stream())
                        .anyMatch(tripInfo -> isSameTrip(tripInfo, trip));
    }

    /**
     * Applies stop cancellations to the trip update
     * @param tripUpdate Trip update
     * @return Trip update with stop cancellations applied
     */
    public GtfsRealtime.TripUpdate applyStopCancellations(GtfsRealtime.TripUpdate tripUpdate) {
        //Keep track of trips that have produced trip updates (to avoid creating NO_DATA trip updates for them)
        tripsWithTripUpdates.put(tripUpdate.getTrip().toBuilder().clearScheduleRelationship().clearTripId().build(), true);

        final GtfsRealtime.TripDescriptor trip = tripUpdate.getTrip();

        if (tripUpdate.getStopTimeUpdateCount() == 0) {
            //No stop time updates, no stops to cancel
            LOG.debug("Trip {} / {} / {} / {} had no stop time updates, cannot apply stop cancellations", trip.getRouteId(), trip.getStartDate(), trip.getStartTime(), trip.getDirectionId());
            return tripUpdate;
        }

        List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream().map(stopTimeUpdate -> {
            if (!stopTimeUpdate.hasStopId()) {
                LOG.debug("Cannot apply stop cancellations for stop time updates without stop ID (trip {} / {} / {} / {})", trip.getRouteId(), trip.getStartDate(), trip.getStartTime(), trip.getDirectionId());
                return stopTimeUpdate;
            }

            if (stopTimeUpdate.hasScheduleRelationship() && stopTimeUpdate.getScheduleRelationship() != GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED) {
                //Cannot apply cancellations to stop time updates that have no data or that are already cancelled
                return stopTimeUpdate;
            }

            List<InternalMessages.StopCancellations.StopCancellation> cancellationsForStop = stopCancellationsByStopId.getOrDefault(stopTimeUpdate.getStopId(), Collections.emptyList());
            if (cancellationsForStop.stream().anyMatch(stopCancellation -> affectsStopTimeUpdate(stopCancellation, tripUpdate.getTrip(), stopTimeUpdate))) {
                return stopTimeUpdate.toBuilder().setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED).build();
            } else {
                return stopTimeUpdate;
            }
        }).collect(Collectors.toList());

        //Replace original stop time updates with ones where stop cancellations have been applied
        return tripUpdate.toBuilder().clearStopTimeUpdate().addAllStopTimeUpdate(stopTimeUpdates).build();
    }

    public static class TripUpdateWithId {
        public final String id;
        public final GtfsRealtime.TripUpdate tripUpdate;

        public TripUpdateWithId(String id, GtfsRealtime.TripUpdate tripUpdate) {
            this.id = id;
            this.tripUpdate = tripUpdate;
        }
    }
}
