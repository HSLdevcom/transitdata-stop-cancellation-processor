package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StopCancellationProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StopCancellationProcessor.class);

    private Map<String, List<InternalMessages.StopCancellations.StopCancellation>> stopCancellationsByStopId;
    private Map<String, InternalMessages.JourneyPattern> journeyPatternById;

    public void updateStopCancellations(InternalMessages.StopCancellations stopCancellations) {
        this.stopCancellationsByStopId = stopCancellations.getStopCancellationsList().stream().collect(Collectors.groupingBy(InternalMessages.StopCancellations.StopCancellation::getStopId));
        this.journeyPatternById = stopCancellations.getAffectedJourneyPatternsList().stream().collect(Collectors.toMap(InternalMessages.JourneyPattern::getJourneyPatternId, Function.identity()));
    }

    static boolean isSameTrip(InternalMessages.TripInfo tripInfo, GtfsRealtime.TripDescriptor tripDescriptor) {
        return RouteIdUtils.normalizeRouteId(tripInfo.getRouteId()).equals(tripDescriptor.getRouteId()) &&
                tripInfo.getStartTime().equals(tripDescriptor.getStartTime()) &&
                tripInfo.getOperatingDay().equals(tripDescriptor.getStartDate()) &&
                PubtransFactory.joreDirectionToGtfsDirection(tripInfo.getDirectionId()) == tripDescriptor.getDirectionId();
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
        if (tripUpdate.getStopTimeUpdateCount() == 0) {
            //No stop time updates, no stops to cancel
            GtfsRealtime.TripDescriptor trip = tripUpdate.getTrip();
            LOG.debug("Trip {} / {} / {} / {} had no stop time updates, cannot apply stop cancellations", trip.getRouteId(), trip.getStartDate(), trip.getStartTime(), trip.getDirectionId());
            return tripUpdate;
        }

        List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates = tripUpdate.getStopTimeUpdateList().stream().map(stopTimeUpdate -> {
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
}
