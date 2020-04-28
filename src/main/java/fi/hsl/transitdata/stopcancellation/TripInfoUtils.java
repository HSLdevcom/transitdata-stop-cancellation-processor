package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.PubtransFactory;
import fi.hsl.common.transitdata.RouteIdUtils;
import fi.hsl.common.transitdata.proto.InternalMessages;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TripInfoUtils {
    private TripInfoUtils() {}

    public static GtfsRealtime.TripDescriptor toTripDescriptor(InternalMessages.TripInfo tripInfo) {
        // TODO: make sure that ScheduleRelationship needs to be set to SCHEDULED
        return GtfsRealtime.TripDescriptor.newBuilder()
                .setRouteId(RouteIdUtils.normalizeRouteId(tripInfo.getRouteId()))
                .setScheduleRelationship(GtfsRealtime.TripDescriptor.ScheduleRelationship.SCHEDULED)
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

    public static long getUnixStartTimeFromTripInfo(InternalMessages.TripInfo tripInfo) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss");
        LocalDateTime dt = LocalDateTime.parse(tripInfo.getOperatingDay()+"-"+tripInfo.getStartTime(), formatter);
        return dt.atZone(ZoneId.of("Europe/Helsinki")).toEpochSecond();
    }

    public static String getFirstStopId(InternalMessages.JourneyPattern journeyPattern) {
        InternalMessages.JourneyPattern.Stop firstStop = journeyPattern.getStopsList().stream()
            .filter(stop -> 1 == stop.getStopSequence())
            .findAny()
            .orElse(null);
        return firstStop != null ? firstStop.getStopId() : null;
    }

    //TODO: this should be in common as rail-tripupdate-source
    public static String getTrainEntityId(GtfsRealtime.TripDescriptor tripDescriptor) {
        return "rail_" + String.join("-", tripDescriptor.getRouteId(), tripDescriptor.getStartDate(), tripDescriptor.getStartTime(), String.valueOf(tripDescriptor.getDirectionId()));
    }
}
