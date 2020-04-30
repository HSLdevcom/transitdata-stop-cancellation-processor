package fi.hsl.transitdata.stopcancellation;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
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
    private Cache<TripIdentifier, Boolean> tripsWithTripUpdates = Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(10)).build();

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
     * Builds trip updates for trips that have cancelled stops but have not produced trip updates recently
     * @param timestamp Timestamp (in seconds) to be used in trip updates
     * @return List of trip updates
     */
    public Collection<TripUpdateWithId> getStopCancellationTripUpdates(final long timestamp) {
        return journeyPatternById.values().stream().map(journeyPattern -> {
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
                if (Boolean.TRUE.equals(tripsWithTripUpdates.getIfPresent(TripIdentifier.fromTripInfo(tripInfo)))) {
                    //Trip had already published trip update, no need to create trip update with NO_DATA
                    continue;
                }

                final GtfsRealtime.TripDescriptor tripDescriptor = toTripDescriptor(tripInfo);

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

                    if (stopCancellationsByStopId.getOrDefault(stop.getStopId(), Collections.emptyList()).stream().anyMatch(stopCancellation -> {
                        return stopCancellation.getAffectedJourneyPatternIdsList().contains(journeyPatternId);
                    })) {
                        return stopTimeUpdate.toBuilder().setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED).build();
                    } else {
                        return stopTimeUpdate;
                    }
                }).collect(Collectors.toList());

        //Replace original stop time updates with ones where stop cancellations have been applied
        return tripUpdate.toBuilder().clearStopTimeUpdate().addAllStopTimeUpdate(stopTimeUpdateList).build();
    }

    public static class TripUpdateWithId {
        public final String id;
        public final GtfsRealtime.TripUpdate tripUpdate;

        public TripUpdateWithId(String id, GtfsRealtime.TripUpdate tripUpdate) {
            this.id = id;
            this.tripUpdate = tripUpdate;
        }
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
