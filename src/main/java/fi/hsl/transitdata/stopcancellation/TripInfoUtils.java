package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;

public class TripInfoUtils {
    private TripInfoUtils() {}

    //TODO: this should be in common as rail-tripupdate-source
    public static String getTrainEntityId(GtfsRealtime.TripDescriptor tripDescriptor) {
        return "rail_" + String.join("-", tripDescriptor.getRouteId(), tripDescriptor.getStartDate(), tripDescriptor.getStartTime(), String.valueOf(tripDescriptor.getDirectionId()));
    }
}
