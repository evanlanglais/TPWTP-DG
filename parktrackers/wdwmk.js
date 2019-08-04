const tp = require('themeparks');
const ParkTracker = require('./parktracker');
const park_id = "wdwmk";
const tracking_rides = [
    'WaltDisneyWorldMagicKingdom_16491297',
    'WaltDisneyWorldMagicKingdom_16767263',
    'WaltDisneyWorldMagicKingdom_16767284',
    'WaltDisneyWorldMagicKingdom_17272158',
    'WaltDisneyWorldMagicKingdom_80010110',
    'WaltDisneyWorldMagicKingdom_80010114',
    'WaltDisneyWorldMagicKingdom_80010153',
    'WaltDisneyWorldMagicKingdom_80010176',
    'WaltDisneyWorldMagicKingdom_80010190',
    'WaltDisneyWorldMagicKingdom_80010192',
    'WaltDisneyWorldMagicKingdom_80010208',
    'WaltDisneyWorldMagicKingdom_80010213',
    'WaltDisneyWorldMagicKingdom_80010222'
];

class WDWMKParkTracker extends ParkTracker {
    constructor() {
        super(new tp.Parks.WaltDisneyWorldMagicKingdom(), park_id, tracking_rides);
    }
}

module.exports = new WDWMKParkTracker();