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

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static fi.hsl.transitdata.stopcancellation.TripInfoUtils.*;

public class StopCancellationProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StopCancellationProcessor.class);

    private final ZoneId timezone;

    private Map<String, List<InternalMessages.StopCancellations.StopCancellation>> stopCancellationsByStopId = Collections.emptyMap();
    private Map<String, InternalMessages.JourneyPattern> journeyPatternById = Collections.emptyMap();
    //Maps trips that are affected by any stop cancellation to journey patterns
    private Map<TripIdentifier, String> journeyPatternIdByTripIdentifier = Collections.emptyMap();

    //Cache of trips that have produced trip updates
    private final Cache<TripIdentifier, Boolean> tripsWithTripUpdates;

    //Cache of trips that we have sent trip updates with cancelled stops
    private final Cache<TripIdentifier, String> tripsWithCancellations;

    public StopCancellationProcessor(ZoneId timezone, Duration tripWithTripUpdateCacheDuration, Duration tripWithCancellationCacheDuration) {
        tripsWithTripUpdates = Caffeine.newBuilder().expireAfterWrite(tripWithTripUpdateCacheDuration).build();
        tripsWithCancellations = Caffeine.newBuilder().expireAfterWrite(tripWithCancellationCacheDuration).build();

        this.timezone = timezone;
    }

    public void updateStopCancellations(InternalMessages.StopCancellations stopCancellations) {
        this.stopCancellationsByStopId = stopCancellations.getStopCancellationsList().stream().collect(Collectors.groupingBy(InternalMessages.StopCancellations.StopCancellation::getStopId));
        this.journeyPatternById = stopCancellations.getAffectedJourneyPatternsList().stream().collect(Collectors.toMap(InternalMessages.JourneyPattern::getJourneyPatternId, Function.identity()));

        this.journeyPatternIdByTripIdentifier = new HashMap<>();
        stopCancellations.getStopCancellationsList().forEach(stopCancellation -> {
            if (stopCancellation.getCause() == InternalMessages.StopCancellations.Cause.JOURNEY_DETOUR) {
                //Only single trip affected
                journeyPatternIdByTripIdentifier.put(TripIdentifier.fromTripInfo(stopCancellation.getAffectedTrip()), stopCancellation.getAffectedJourneyPatternIds(0));
            } else {
                final Instant from = Instant.ofEpochSecond(stopCancellation.getValidFromUnixS());
                final Instant to = Instant.ofEpochSecond(stopCancellation.getValidToUnixS());

                stopCancellation.getAffectedJourneyPatternIdsList().stream().map(journeyPatternById::get).forEach(journeyPattern -> {
                    journeyPattern.getTripsList().forEach(tripInfo -> {
                        final TripIdentifier tripIdentifier = TripIdentifier.fromTripInfo(tripInfo);
                        final Instant tripStartTime = tripIdentifier.getZonedStartTime(timezone).toInstant();

                        //Add only trips that are affected by stop cancellations
                        if (tripStartTime.isAfter(from) && tripStartTime.isBefore(to)) {
                            journeyPatternIdByTripIdentifier.put(TripIdentifier.fromTripInfo(tripInfo), journeyPattern.getJourneyPatternId());
                        }
                    });
                });
            }
        });
    }

    /**
     * Creates trip updates for trips that had stop cancellations that were later cancelled (cancellation-of-cancellation)
     * @param timestamp Timestamp used in trip update
     * @return List of trip updates
     */
    private Collection<TripUpdateWithId> getTripUpdatesForCancellationsOfCancellations(final long timestamp) {
        return tripsWithCancellations.asMap().entrySet().stream()
                //Trip does not have any active cancellations
                .filter(tripIdentifierAndId -> !journeyPatternIdByTripIdentifier.containsKey(tripIdentifierAndId.getKey()))
                //Trip has not sent trip updates (which would effectively cancel cancellations)
                .filter(tripIdentifierAndId -> !Boolean.TRUE.equals(tripsWithTripUpdates.getIfPresent(tripIdentifierAndId.getKey())))
                .map(tripIdentifierAndId -> {
                    //Create trip update that cancels all stop cancellations
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

                    //NO_DATA stop time updates for the journey pattern
                    final List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = journeyPattern.getStopsList().stream()
                            .sorted(Comparator.comparingInt(InternalMessages.JourneyPattern.Stop::getStopSequence))
                            .map(InternalMessages.JourneyPattern.Stop::getStopId)
                            .map(StopCancellationProcessor::createNoDataStopTimeUpdate)
                            .collect(Collectors.toList());

                    for (InternalMessages.TripInfo tripInfo : journeyPattern.getTripsList()) {
                        final TripIdentifier tripIdentifier = TripIdentifier.fromTripInfo(tripInfo);

                        if (Boolean.TRUE.equals(tripsWithTripUpdates.getIfPresent(tripIdentifier))) {
                            //Trip had already published trip update, no need to create trip update with NO_DATA
                            continue;
                        }

                        final Collection<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdatesWithCancellations = stopTimeUpdates.stream()
                                .map(stopTimeUpdate -> {
                                    final boolean isStopCancelled = stopCancellationsByStopId.getOrDefault(stopTimeUpdate.getStopId(), Collections.emptyList())
                                            .stream()
                                            .anyMatch(isTripAffectedByStopCancellation(tripIdentifier, journeyPatternIdByTripIdentifier.get(tripIdentifier)));

                                    return isStopCancelled ?
                                            stopTimeUpdate.toBuilder().setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED).build() :
                                            stopTimeUpdate;
                                }).collect(Collectors.toList());

                        final boolean hasStopCancellations = stopTimeUpdatesWithCancellations.stream()
                                .map(GtfsRealtime.TripUpdate.StopTimeUpdate::getScheduleRelationship)
                                .anyMatch(scheduleRelationship -> scheduleRelationship == GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED);

                        if (hasStopCancellations) {
                            final GtfsRealtime.TripDescriptor tripDescriptor = tripIdentifier.toGtfsTripDescriptor();

                            final String entityId = RouteIdUtils.isTrainRoute(tripInfo.getRouteId()) ?
                                    getTrainEntityId(tripDescriptor) :
                                    tripInfo.getTripId();

                            //Add cancellation to cache so that it can be cancelled later (cancellation-of-cancellation)
                            tripsWithCancellations.put(tripIdentifier, entityId);

                            final GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                                    .setTrip(tripDescriptor)
                                    .setTimestamp(timestamp)
                                    .addAllStopTimeUpdate(stopTimeUpdatesWithCancellations)
                                    .build();

                            tripUpdates.add(new TripUpdateWithId(entityId, tripUpdate));
                        }
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

        //Sort trip updates so that updates for earlier trips will be sent earlier
        tripUpdates.sort(Comparator.comparing(tripUpdateWithId -> TripInfoUtils.getStartTime(tripUpdateWithId.tripUpdate.getTrip())));

        /*final Set<String> tripIds = tripUpdates.stream().map(tripUpdateWithId -> tripUpdateWithId.id).collect(Collectors.toSet());
        LOG.debug("Distinct trip IDs: " + tripIds.size());*/

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
        final TripIdentifier tripIdentifier = TripIdentifier.fromGtfsTripDescriptor(tripUpdate.getTrip());

        //Keep track of trips that have produced trip updates (to avoid creating NO_DATA trip updates for them)
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
                            createNoDataStopTimeUpdate(stop.getStopId());

                    final boolean stopCancelled = stopCancellationsByStopId.getOrDefault(stop.getStopId(), Collections.emptyList()).stream()
                            .anyMatch(isTripAffectedByStopCancellation(tripIdentifier, journeyPatternId));

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

    private Predicate<InternalMessages.StopCancellations.StopCancellation> isTripAffectedByStopCancellation(final TripIdentifier tripIdentifier, final String journeyPatternId) {
        return stopCancellation -> {
            //Stop cancellation affects only a single trip
            if (stopCancellation.getCause() == InternalMessages.StopCancellations.Cause.JOURNEY_DETOUR) {
                return TripIdentifier.fromTripInfo(stopCancellation.getAffectedTrip()).equals(tripIdentifier);
            } else {
                //Check if trip is affected by stop cancellation
                final Instant from = Instant.ofEpochSecond(stopCancellation.getValidFromUnixS());
                final Instant to = Instant.ofEpochSecond(stopCancellation.getValidToUnixS());

                final Instant tripStartTime = tripIdentifier.getZonedStartTime(timezone).toInstant();

                return from.isBefore(tripStartTime) && to.isAfter(tripStartTime) && stopCancellation.getAffectedJourneyPatternIdsList().contains(journeyPatternId);
            }
        };
    }

    private static GtfsRealtime.TripUpdate.StopTimeUpdate createNoDataStopTimeUpdate(final String stopId) {
        return GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                .setStopId(stopId)
                .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA)
                .build();
    }

    private static GtfsRealtime.TripUpdate.StopTimeUpdate createSkippedStopTimeUpdate(final String stopId) {
        return GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                .setStopId(stopId)
                .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED)
                .build();
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

        public ZonedDateTime getZonedStartTime(ZoneId zoneId) {
            final LocalDate date = LocalDate.parse(operatingDate, DateTimeFormatter.BASIC_ISO_DATE);

            final String[] timeParts = startTime.split(":");
            final int hours = Integer.parseInt(timeParts[0]);
            final int minutes = Integer.parseInt(timeParts[1]);
            final int seconds = Integer.parseInt(timeParts[2]);

            final LocalDateTime localDateTime = hours >= 24 ?
                    date.plusDays(1).atTime(hours - 24, minutes, seconds) :
                    date.atTime(hours, minutes, seconds);

            return localDateTime.atZone(zoneId);
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
