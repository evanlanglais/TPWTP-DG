const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const tpi = new tp.Parks.UniversalIslandsOfAdventure();
const park_id = "uioa";

class UIOAParkTracker extends ParkTracker {
    constructor() {
        super(tpi, park_id);
    }
}

module.exports = new UIOAParkTracker();
