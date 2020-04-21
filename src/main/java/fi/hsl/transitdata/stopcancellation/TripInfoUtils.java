package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;

public class TripInfoUtils {
    private TripInfoUtils() {}

    public static GtfsRealtime.TripDescriptor toTripDescriptor(InternalMessages.TripInfo tripInfo) {
        return GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(RouteIdUtils.normalizeRouteId(tripInfo.getRouteId()))
                .setStartDate(tripInfo.getOperatingDay())
                .setStartTime(tripInfo.getStartTime())
                .setDirectionId(PubtransFactory.joreDirectionToGtfsDirection(tripInfo.getDirectionId()))
                .build();
    }

    public static boolean isSameTrip(InternalMessages.TripInfo tripInfo, GtfsRealtime.TripDescriptor tripDescriptor) {
        GtfsRealtime.TripDescriptor fromTripInfo = toTripDescriptor(tripInfo);
        return fromTripInfo.getRouteId().equals(tripDescriptor.getRouteId()) &&
                fromTripInfo.getStartDate().equals(tripDescriptor.getStartDate()) &&
                fromTripInfo.getStartTime().equals(tripDescriptor.getStartTime()) &&
                fromTripInfo.getDirectionId() == tripDescriptor.getDirectionId();
    }

    //TODO: this should be in common as rail-tripupdate-source
    public static String getTrainEntityId(GtfsRealtime.TripDescriptor tripDescriptor) {
        return "rail_" + String.join("-", tripDescriptor.getRouteId(), tripDescriptor.getStartDate(), tripDescriptor.getStartTime(), String.valueOf(tripDescriptor.getDirectionId()));
    }
}
