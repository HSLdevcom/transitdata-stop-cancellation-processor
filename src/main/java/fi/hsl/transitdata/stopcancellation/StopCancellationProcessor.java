package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StopCancellationProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StopCancellationProcessor.class);

    private Map<String, List<InternalMessages.StopCancellation>> stopCancellations;

    public void updateStopCancellations(InternalMessages.StopCancellations stopCancellations) {
        this.stopCancellations = stopCancellations.getStopCancellationsList().stream().collect(Collectors.groupingBy(InternalMessages.StopCancellation::getStopId));
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
            if (!stopTimeUpdate.hasScheduleRelationship() || stopTimeUpdate.getScheduleRelationship() != GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED) {
                //Cannot apply cancellations to stop time updates that have no data or that are already cancelled
                return stopTimeUpdate;
            }

            List<InternalMessages.StopCancellation> cancellationsForStop = stopCancellations.getOrDefault(stopTimeUpdate.getStopId(), Collections.emptyList());
            if (cancellationsForStop.stream().anyMatch(cancellationForStop -> {
                return stopTimeUpdate.getDeparture().getTime() >= cancellationForStop.getValidFromUtcMs() &&
                        stopTimeUpdate.getArrival().getTime() <= cancellationForStop.getValidToUtcMs();
            })) {
                return stopTimeUpdate.toBuilder().setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED).build();
            } else {
                return stopTimeUpdate;
            }
        }).collect(Collectors.toList());

        //Replace original stop time updates with ones where stop cancellations have been applied
        return tripUpdate.toBuilder().clearStopTimeUpdate().addAllStopTimeUpdate(stopTimeUpdates).build();
    }
}
