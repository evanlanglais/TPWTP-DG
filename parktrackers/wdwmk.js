const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const tpi = new tp.Parks.WaltDisneyWorldMagicKingdom();
const park_id = "wdwmk";

class WDWMKParkTracker extends ParkTracker {
    constructor() {
        super(tpi, park_id);
    }
}

module.exports = new WDWMKParkTracker();
