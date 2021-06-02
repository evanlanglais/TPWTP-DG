const MongoClient = require('mongodb').MongoClient;
const moment = require('moment');
const dotenv = require('dotenv');

dotenv.config();

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

let dbs = {};

function registerPark(park_id) {
    return client.connect()
        .then((client) => {
            // console.log("we did it?");
            dbs[park_id] = {};
            dbs[park_id].db = client.db(park_id);
            dbs[park_id].rides = {};
        })
        .catch((err) => {
            console.log("Cannot connect to mongodb!");
            console.error(err);
        });
}

function registerRide(park_id, ride_id) {
    console.log(`Registering ${ride_id}`);
    return Promise.resolve(dbs[park_id].rides[ride_id] = dbs[park_id].db.collection(ride_id));
}

function submitWaitTime(park_id, ride, park_time) {
    const collection = dbs[park_id].rides[ride.id];
    const timestamp = `[${moment().format('LT')} local | ${park_time.format('LT')} park]`;
    const park_date = new moment(park_time).startOf('day');
    const date_index = park_date.toISOString();
    return new Promise((resolve, reject) => {
        collection.findOne({date: date_index}, async (err, result) => {
            if (err) {
                console.error(timestamp, err);
                return reject(err);
            }
            if (!result) {
                try {
                    console.log(timestamp, `Creating wait time doc for ${ride.id}`);
                    await createWaitTimeDocument(collection, ride);
                    console.log(timestamp, `Finished creating wait time doc for ${ride.id}`);
                } catch (err) {
                    console.error(timestamp, err);
                    return reject(err);
                }
            }

            const time_index = momentToIndex(park_time);
            console.log(timestamp, `Submitting wait time at time index ${time_index} (${indexToMoment(moment(date_index), time_index).format('LT')}) for ${ride.id}`);
            let update_obj = { $set: {}};
            update_obj.$set[`wait_times.${time_index}`] = ride.waitTime;
            collection.updateOne({date: date_index}, update_obj, (err, result) => {
                if (err) {
                    console.log(timestamp, `Failed to submit wait time for ${ride.id}`);
                    console.error(err);
                    return reject(err);
                } else {
                    console.log(timestamp, `Successfully submitted wait time of ${ride.waitTime} for ${ride.id}`);
                    return resolve(result.result);
                }

            })
        });
    });
}


function createWaitTimeDocument(collection, ride, time = moment()) {
    const time_doc = {
        ride_id: ride.id,
        ride_name: ride.name,
        date: time.startOf('day').toISOString(),
        month: time.month(),
        week: time.week(),
        day: time.date(),
        dayofweek: time.day(),
        dayofyear: time.dayOfYear(),
        wait_times: {},
    };
    return new Promise((resolve, reject) => {
        collection.insertOne(time_doc, (err, result) => {
            if (err)
                reject(err);
            resolve(result);
        });
    });
}

function momentToIndex(time) {
    let five_count = time.minute();
    const remainder = five_count % 5;
    if (remainder !== 0) {
        five_count -= remainder;
    }
    return (time.hour() * 12) + (five_count / 5);
}

function indexToMoment(time_base, time_index) {
    return time_base.add(time_index * 5, 'm');
}

const metadata_db = 'trackingconfig';
const park_metadata_collection = 'park_metadata';

async function GetRideIdsForPark(park_id)
{
    try
    {
        await client.connect();
        const db = client.db(metadata_db);
        const park_metadata = db.collection(park_metadata_collection);

        let park_data = await park_metadata.findOne({ id: park_id }, { projection: {rides: 1}});

        return park_data.rides;
    } catch (e) {
        console.error('Unable to get rides for park!', e);
    }
}

module.exports =
{
    registerPark,
    registerRide,
    submitWaitTime,
    GetRideIdsForPark
}
