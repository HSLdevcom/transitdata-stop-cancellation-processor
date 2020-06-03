package fi.hsl.transitdata.stopcancellation.models;

import com.google.transit.realtime.GtfsRealtime;

public class TripUpdateWithId {
    public final String id;
    public final GtfsRealtime.TripUpdate tripUpdate;

    public TripUpdateWithId(String id, GtfsRealtime.TripUpdate tripUpdate) {
        this.id = id;
        this.tripUpdate = tripUpdate;
    }
}
