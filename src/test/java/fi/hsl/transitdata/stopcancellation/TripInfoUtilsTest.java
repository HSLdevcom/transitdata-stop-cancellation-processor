package fi.hsl.transitdata.stopcancellation;

import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Test;
import static org.junit.Assert.*;

public class TripInfoUtilsTest {

    @Test
    public void testGetDepartureUnixTimeFromTripInfo() {
        InternalMessages.TripInfo tripInfo = InternalMessages.TripInfo.newBuilder()
                .setTripId("1234")
                .setRouteId("1234")
                .setDirectionId(0)
                .setOperatingDay("20200430")
                .setStartTime("04:18:00")
                .build();

        long departureTime = TripInfoUtils.getUnixStartTimeFromTripInfo(tripInfo);
        assertEquals(1588209480, departureTime);
    }
}
