const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const tpi = new tp.Parks.UniversalIslandsOfAdventure();
const park_id = "uioa";
const tracking_rides = [
    'UniversalIslandsOfAdventure_10831',
    'UniversalIslandsOfAdventure_10833',
    'UniversalIslandsOfAdventure_10835',
    'UniversalIslandsOfAdventure_10837',
    'UniversalIslandsOfAdventure_10839',
    'UniversalIslandsOfAdventure_10840',
    'UniversalIslandsOfAdventure_10842',
    'UniversalIslandsOfAdventure_10857',
    'UniversalIslandsOfAdventure_10859',
    'UniversalIslandsOfAdventure_10862',
    'UniversalIslandsOfAdventure_13225',
    'UniversalIslandsOfAdventure_13799',
    'UniversalIslandsOfAdventure_17097'
];

class UIOAParkTracker extends ParkTracker {
    constructor() {
        super(tpi, park_id, tracking_rides);
    }
}

module.exports = new UIOAParkTracker();
