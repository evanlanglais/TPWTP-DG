const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const tpi = new tp.Parks.WaltDisneyWorldEpcot();
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
        super(tpi, park_id, tracking_rides);
    }
}

module.exports = new WDWEParkTracker();
