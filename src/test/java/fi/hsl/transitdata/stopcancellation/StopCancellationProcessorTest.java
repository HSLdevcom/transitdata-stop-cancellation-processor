package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stopcancellation.models.TripUpdateWithId;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.*;

public class StopCancellationProcessorTest {
    private StopCancellationProcessor stopCancellationProcessor;

    @Before
    public void setup() {
        stopCancellationProcessor = new StopCancellationProcessor();

        InternalMessages.JourneyPattern journeyPattern1 = InternalMessages.JourneyPattern.newBuilder()
                .setJourneyPatternId("1")
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("1").setStopSequence(1))
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("2").setStopSequence(2))
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("3").setStopSequence(3))
                .addTrips(InternalMessages.TripInfo.newBuilder().setTripId("1").setRouteId("1001").setOperatingDay("20200101").setStartTime("12:00:00").setDirectionId(1).build())
                .build();

        InternalMessages.JourneyPattern journeyPattern2 = InternalMessages.JourneyPattern.newBuilder()
                .setJourneyPatternId("2")
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("1").setStopSequence(3))
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("2").setStopSequence(2))
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("3").setStopSequence(1))
                .addTrips(InternalMessages.TripInfo.newBuilder().setTripId("2").setRouteId("1001").setOperatingDay("20200101").setStartTime("12:00:00").setDirectionId(2).build())
                .build();

        InternalMessages.StopCancellations stopCancellations = InternalMessages.StopCancellations.newBuilder()
                .addAffectedJourneyPatterns(journeyPattern1)
                .addAffectedJourneyPatterns(journeyPattern2)
                .addStopCancellations(InternalMessages.StopCancellations.StopCancellation.newBuilder()
                        .setStopId("1")
                        .setValidFromUnixS(0)
                        .setValidToUnixS(1000)
                        .addAffectedJourneyPatternIds("1")
                        .addAffectedJourneyPatternIds("2"))
                .addStopCancellations(InternalMessages.StopCancellations.StopCancellation.newBuilder()
                        .setStopId("1")
                        .setValidFromUnixS(2000)
                        .setValidToUnixS(3000))
                .addStopCancellations(InternalMessages.StopCancellations.StopCancellation.newBuilder()
                        .setStopId("2")
                        .setValidFromUnixS(10000)
                        .setValidToUnixS(15000))
                .build();
        stopCancellationProcessor.updateStopCancellations(stopCancellations);
    }

    @Test
    public void testScheduleRelationshipIsNotChangedForStopThatIsNotCancelled() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("1001").setStartDate("20200101").setStartTime("12:00:00").setDirectionId(0).build())
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

        GtfsRealtime.TripUpdate tripUpdateWithStopCancellations = stopCancellationProcessor.applyStopCancellations(tripUpdate);
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
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("1001").setStartDate("20200101").setStartTime("12:00:00").setDirectionId(0).build())
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

        GtfsRealtime.TripUpdate tripUpdateWithStopCancellations = stopCancellationProcessor.applyStopCancellations(tripUpdate);
        Optional<GtfsRealtime.TripUpdate.StopTimeUpdate> stop1 = tripUpdateWithStopCancellations.getStopTimeUpdateList().stream()
                .filter(stu -> "1".equals(stu.getStopId()))
                .findAny();

        assertTrue(stop1.isPresent());
        assertEquals("1", stop1.get().getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED, stop1.get().getScheduleRelationship());
    }

    @Test
    public void testNoDataTripUpdatesAreCreated() {
        final long timestamp = System.currentTimeMillis();

        Collection<TripUpdateWithId> tripUpdates = stopCancellationProcessor.getStopCancellationTripUpdates(timestamp);

        assertEquals(2, tripUpdates.size());

        Optional<GtfsRealtime.TripUpdate> maybeTrip1 = tripUpdates.stream()
                .filter(tripUpdateWithId -> "1".equals(tripUpdateWithId.id))
                .map(tripUpdateWithId -> tripUpdateWithId.tripUpdate)
                .findAny();

        assertTrue(maybeTrip1.isPresent());

        GtfsRealtime.TripUpdate trip1 = maybeTrip1.get();

        assertEquals(3, trip1.getStopTimeUpdateCount());

        assertEquals("1", trip1.getStopTimeUpdate(0).getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED, trip1.getStopTimeUpdate(0).getScheduleRelationship());
        assertEquals("2", trip1.getStopTimeUpdate(1).getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA, trip1.getStopTimeUpdate(1).getScheduleRelationship());
        assertEquals("3", trip1.getStopTimeUpdate(2).getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.NO_DATA, trip1.getStopTimeUpdate(2).getScheduleRelationship());
    }

}
