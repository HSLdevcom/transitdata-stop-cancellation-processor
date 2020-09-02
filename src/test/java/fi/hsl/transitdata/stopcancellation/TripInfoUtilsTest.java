package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class TripInfoUtilsTest {
    @Test
    public void testGetStartTime() {
        LocalDateTime dateTime1 = TripInfoUtils.getStartTime(GtfsRealtime.TripDescriptor.newBuilder().setStartDate("20200101").setStartTime("10:00:00").build());
        assertEquals(LocalDateTime.of(2020, 1, 1, 10, 0), dateTime1);

        LocalDateTime dateTime2 = TripInfoUtils.getStartTime(GtfsRealtime.TripDescriptor.newBuilder().setStartDate("20200101").setStartTime("27:30:00").build());
        assertEquals(LocalDateTime.of(2020, 1, 2, 3, 30), dateTime2);
    }
}
