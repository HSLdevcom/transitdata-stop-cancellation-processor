package fi.hsl.transitdata.stopcancellation;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.transitdata.stopcancellation.models.TripUpdateWithId;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.*;

public class StopCancellationProcessorTest {
    private static final ZoneId timezone = ZoneId.of("Europe/Helsinki");

    private StopCancellationProcessor stopCancellationProcessor;

    //TODO: update tests: multiple stop cancellations for same trip should work

    @Before
    public void setup() {
        stopCancellationProcessor = new StopCancellationProcessor(timezone);

        InternalMessages.JourneyPattern journeyPattern1 = InternalMessages.JourneyPattern.newBuilder()
                .setJourneyPatternId("1")
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("1").setStopSequence(1))
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("2").setStopSequence(2))
                .addStops(InternalMessages.JourneyPattern.Stop.newBuilder().setStopId("3").setStopSequence(3))
                .addTrips(InternalMessages.TripInfo.newBuilder().setTripId("1").setRouteId("1001").setOperatingDay("20200101").setStartTime("12:00:00").setDirectionId(1).build())
                .addTrips(InternalMessages.TripInfo.newBuilder().setTripId("1").setRouteId("1001").setOperatingDay("20200101").setStartTime("14:00:00").setDirectionId(1).build())
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
                        .setCause(InternalMessages.StopCancellations.Cause.CLOSED_STOP)
                        .setStopId("1")
                        .setValidFromUnixS(LocalDate.of(2020, 1, 1).atTime(11, 0).atZone(timezone).toEpochSecond())
                        .setValidToUnixS(LocalDate.of(2020, 1, 1).atTime(13, 0).atZone(timezone).toEpochSecond())
                        .addAffectedJourneyPatternIds("1")
                        .addAffectedJourneyPatternIds("2"))
                /*.addStopCancellations(InternalMessages.StopCancellations.StopCancellation.newBuilder()
                        .setStopId("1")
                        .setValidFromUnixS(2000)
                        .setValidToUnixS(3000))
                .addStopCancellations(InternalMessages.StopCancellations.StopCancellation.newBuilder()
                        .setStopId("2")
                        .setValidFromUnixS(10000)
                        .setValidToUnixS(15000))*/
                .build();
        stopCancellationProcessor.updateStopCancellations(stopCancellations);
    }

    @Test
    public void testScheduleRelationshipIsNotChangedForStopThatIsNotCancelled() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("1001").setStartDate("20200101").setStartTime("12:00:00").setDirectionId(0).build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("1")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(0).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(10).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("2")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("3")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
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
    public void testScheduleRelationshipIsNotChangedWhenStopCancellationIsNotValid() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("1001").setStartDate("20200101").setStartTime("14:00:00").setDirectionId(0).build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("1")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(0).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(10).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("2")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("3")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .build();

        GtfsRealtime.TripUpdate tripUpdateWithStopCancellations = stopCancellationProcessor.applyStopCancellations(tripUpdate);
        Optional<GtfsRealtime.TripUpdate.StopTimeUpdate> stop1 = tripUpdateWithStopCancellations.getStopTimeUpdateList().stream()
                .filter(stu -> "1".equals(stu.getStopId()))
                .findAny();

        assertTrue(stop1.isPresent());
        assertEquals("1", stop1.get().getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED, stop1.get().getScheduleRelationship());
    }

    @Test
    public void testScheduleRelationshipIsChangedForStopIsCancelled() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("1001").setStartDate("20200101").setStartTime("12:00:00").setDirectionId(0).build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("1")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(0).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(10).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("2")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("3")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
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
    public void testCancellationOfCancellation() {
        GtfsRealtime.TripUpdate tripUpdate = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("1001").setStartDate("20200101").setStartTime("12:00:00").setDirectionId(0).build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("1")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(0).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(10).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("2")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(120).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                        .setStopId("3")
                        .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
                        .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setDelay(80).build())
                        .setScheduleRelationship(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED)
                        .build())
                .build();

        GtfsRealtime.TripUpdate tripUpdateWithStopCancellations = stopCancellationProcessor.applyStopCancellations(tripUpdate);
        Optional<GtfsRealtime.TripUpdate.StopTimeUpdate> stop1 = tripUpdateWithStopCancellations.getStopTimeUpdateList().stream()
                .filter(stu -> "1".equals(stu.getStopId()))
                .findAny();

        //Test that stop was cancelled
        assertTrue(stop1.isPresent());
        assertEquals("1", stop1.get().getStopId());
        assertEquals(GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED, stop1.get().getScheduleRelationship());

        //Cancel all stop cancellations
        stopCancellationProcessor.updateStopCancellations(InternalMessages.StopCancellations.newBuilder().build());

        //Test that no stops are cancelled any more
        GtfsRealtime.TripUpdate tripUpdateAfterCancellationOfCancellation = stopCancellationProcessor.applyStopCancellations(tripUpdate);
        assertEquals(3, tripUpdateAfterCancellationOfCancellation.getStopTimeUpdateList().stream()
                .filter(stopTimeUpdate -> stopTimeUpdate.getScheduleRelationship() != GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED)
                .count());
    }

    @Test
    public void testNoDataTripUpdatesAreCreated() {
        final long timestamp = System.currentTimeMillis() / 1000;

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

    @Test
    public void testNoDataTripUpdatesAreCancelledAfterStopCancellation() {
        final long timestamp = System.currentTimeMillis() / 1000;

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

        //Cancel all stop cancellations
        stopCancellationProcessor.updateStopCancellations(InternalMessages.StopCancellations.newBuilder().build());

        Collection<TripUpdateWithId> tripUpdatesWithCancellationsOfCancellations = stopCancellationProcessor.getStopCancellationTripUpdates(timestamp + 1);

        assertEquals(2, tripUpdatesWithCancellationsOfCancellations.size());
        assertEquals(2, tripUpdatesWithCancellationsOfCancellations.stream()
                .filter(tripUpdateWithId -> {
                    GtfsRealtime.TripUpdate tripUpdate = tripUpdateWithId.tripUpdate;

                    return tripUpdate.getStopTimeUpdateCount() == 1 && tripUpdate.getStopTimeUpdate(0).getScheduleRelationship() != GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship.SKIPPED;
                })
                .count());
    }

}
