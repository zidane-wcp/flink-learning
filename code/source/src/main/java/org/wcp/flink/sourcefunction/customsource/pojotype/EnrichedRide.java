package org.wcp.flink.sourcefunction.customsource.pojotype;


import org.wcp.flink.sourcefunction.customsource.utils.GeoUtils;

public class EnrichedRide extends TaxiRide {
    public int startCell;
    public int endCell;
    public EnrichedRide() {}
    public EnrichedRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.eventTime = ride.eventTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;
        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    public String toString() {
        return super.toString() + "," +
                Integer.toString(this.startCell) + "," +
                Integer.toString(this.endCell);
    }
}