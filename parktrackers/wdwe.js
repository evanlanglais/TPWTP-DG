const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const tpi = new tp.Parks.WaltDisneyWorldEpcot();
const park_id = "wdwe";

class WDWEParkTracker extends ParkTracker {
    constructor() {
        super(tpi, park_id);
    }
}

module.exports = new WDWEParkTracker();
