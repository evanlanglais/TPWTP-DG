const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const park_id = "wdwe";
const tracking_rides = [
    'WaltDisneyWorldEpcot_20194',
    'WaltDisneyWorldEpcot_107785',
    'WaltDisneyWorldEpcot_80010173',
    'WaltDisneyWorldEpcot_80010191',
    'WaltDisneyWorldEpcot_80010199'
];

class WDWEParkTracker extends ParkTracker {
    constructor() {
        let park = new tp.Parks.WaltDisneyWorldEpcot();
        // park.GetWaitTimes().then((rideTimes) => rideTimes.forEach((ride) => console.log(`${ride.id}: ${ride.name}`)));
        super(park, park_id, tracking_rides);
    }
}

module.exports = new WDWEParkTracker();