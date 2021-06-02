const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const tpi = new tp.Parks.UniversalStudiosFlorida();
const park_id = "usf";

class USFParkTracker extends ParkTracker {
    constructor() {
        super(tpi, park_id);
    }
}

module.exports = new USFParkTracker();
