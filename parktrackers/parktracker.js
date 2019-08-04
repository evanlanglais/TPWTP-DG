const dm = require('../data_manager');

const ParkObject = Symbol('Park Object from themeparks API');
const ParkID = Symbol('Park ID for data tracking purposes');
const TrackingRides = Symbol('Array of ride id\'s for tracking purposes');

class ParkTracker {

    constructor(park_object, park_id, tracking_rides = []) {
        if (this.constructor === ParkTracker) {
            throw new TypeError('Cannot create Parktracker directly');
        }
        this[ParkObject] = park_object;
        this[ParkID] = park_id;
        this[TrackingRides] = tracking_rides;
        this.RegisterPark().then(() => this.StartRideTracking());
    };

    RegisterPark() {
        return dm.registerPark(this[ParkID]).then(() => {
            console.log(`Finished registering ${this[ParkID]}`);
            let promises = [];
            this[TrackingRides].forEach((ride_id) => {
                promises.push(dm.registerRide(this[ParkID], ride_id));
            });
            return Promise.all(promises);
        });
    }

    StartRideTracking() {
        return this[ParkObject].GetWaitTimes()
            .then((rideTimes) => {
                let promises = rideTimes
                    .filter((ride) => this[TrackingRides].includes(ride.id))
                    .filter((ride) => ride.active)
                    .map((ride) => dm.submitWaitTime(this[ParkID], ride));
                return Promise.all(promises);
            })
            .catch((error) => {
                console.error(error);
            })
            .then(() => {
                return setTimeout( () => this.StartRideTracking() , 1000 * 60 * 5); // refresh every 5 minutes
            });
    };
}

module.exports = ParkTracker;