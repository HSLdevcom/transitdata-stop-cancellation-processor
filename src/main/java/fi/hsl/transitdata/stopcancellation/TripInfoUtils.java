package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TripInfoUtils {
    private TripInfoUtils() {}

    //TODO: this should be in common as rail-tripupdate-source
    public static String getTrainEntityId(GtfsRealtime.TripDescriptor tripDescriptor) {
        return "rail_" + String.join("-", tripDescriptor.getRouteId(), tripDescriptor.getStartDate(), tripDescriptor.getStartTime(), String.valueOf(tripDescriptor.getDirectionId()));
    }

    public static LocalDateTime getStartTime(GtfsRealtime.TripDescriptor tripDescriptor) {
        final LocalDate date = LocalDate.parse(tripDescriptor.getStartDate(), DateTimeFormatter.BASIC_ISO_DATE);

        final String[] time = tripDescriptor.getStartTime().split(":");
        final int hours = Integer.parseInt(time[0]);
        final int minutes = Integer.parseInt(time[1]);
        final int seconds = Integer.parseInt(time[2]);

        return date.plusDays(hours / 24).atTime(hours % 24, minutes, seconds);
    }
}
