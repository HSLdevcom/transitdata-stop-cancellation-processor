package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StopCancellationProcessorTest {
    private StopCancellationProcessor stopCancellationProcessor;

    @Before
    public void setup() {
        stopCancellationProcessor = new StopCancellationProcessor();

        InternalMessages.StopCancellations stopCancellations = InternalMessages.StopCancellations.newBuilder()
                .addStopCancellations(InternalMessages.StopCancellation.newBuilder().setStopId("1").setValidFromUtcMs(0).setValidToUtcMs(1000).build())
                .addStopCancellations(InternalMessages.StopCancellation.newBuilder().setStopId("1").setValidFromUtcMs(2000).setValidToUtcMs(3000).build())
                .addStopCancellations(InternalMessages.StopCancellation.newBuilder().setStopId("2").setValidFromUtcMs(10000).setValidToUtcMs(15000).build())
                .build();
        stopCancellationProcessor.updateStopCancellations(stopCancellations);
    }

    @Test
    public void testScheduleRelationshipIsNotChangedForStopThatIsNotCancelled() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId("trip_1").build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("1")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(0L).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(10L).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("2")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(5000L).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(5010L).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("3")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(10000L).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(10010L).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .build();

        GtfsRealtime.TripUpdate tripUpdateWithStopCancellations = stopCancellationProcessor.processStopCancellations(tripUpdate);
        Optional<GtfsRealtime.TripUpdate.StopTimeUpdate> stop3 = tripUpdateWithStopCancellations.getStopTimeUpdateList().stream()
                .filter(stu -> "3".equals(stu.getStopId()))
                .findAny();

        assertTrue(stop3.isPresent());
        assertEquals("3", stop3.get().getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED, stop3.get().getScheduleRelationship());
    }

    @Test
    public void testScheduleRelationshipIsChangedForStopIsCancelled() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId("trip_1").build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId("1")
                    .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(0L).build())
                    .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(10L).build())
                    .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                    .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId("2")
                    .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(5000L).build())
                    .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(5010L).build())
                    .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                    .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                    .setStopId("3")
                    .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(10000L).build())
                    .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(10010L).build())
                    .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                    .build())
                .build();

        GtfsRealtime.TripUpdate tripUpdateWithStopCancellations = stopCancellationProcessor.processStopCancellations(tripUpdate);
        Optional<GtfsRealtime.TripUpdate.StopTimeUpdate> stop1 = tripUpdateWithStopCancellations.getStopTimeUpdateList().stream()
                .filter(stu -> "1".equals(stu.getStopId()))
                .findAny();

        assertTrue(stop1.isPresent());
        assertEquals("1", stop1.get().getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED, stop1.get().getScheduleRelationship());
    }

}
