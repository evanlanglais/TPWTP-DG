const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const tpi = new tp.Parks.UniversalStudiosFlorida();
const park_id = "usf";
const tracking_rides = [
    'UniversalStudiosFlorida_10135',
    'UniversalStudiosFlorida_10838',
    'UniversalStudiosFlorida_10841',
    'UniversalStudiosFlorida_10853',
    'UniversalStudiosFlorida_10858',
    'UniversalStudiosFlorida_10860',
    'UniversalStudiosFlorida_10875',
    'UniversalStudiosFlorida_10877',
    'UniversalStudiosFlorida_10879',
    'UniversalStudiosFlorida_13221',
    'UniversalStudiosFlorida_13228',
    'UniversalStudiosFlorida_14348',
    'UniversalStudiosFlorida_16061',
    'UniversalStudiosFlorida_17924',
];

class USFParkTracker extends ParkTracker {
    constructor() {
        super(tpi, park_id, tracking_rides);
    }
}

module.exports = new USFParkTracker();
