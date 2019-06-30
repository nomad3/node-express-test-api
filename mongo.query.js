const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const { inspect } = require('util');

const url = 'mongodb://localhost:27017';
const dbName = 'fatvStats';
MongoClient.connect(url, function(err, client) {
  assert.equal(null, err);
  const db = client.db(dbName);
  const conversation_turns = db.collection('conversation_turns');
  conversation_turns.createIndex({ 'entities.start_time': -1 })
    .then(() => conversation_turns.aggregate([
      // UNWINDING entities
      { $unwind: {
          path: '$entities',
          preserveNullAndEmptyArrays: true,
      } },
      // ONLY THOSE entities IN FEBURARY
      { $match: {
        $or: [
          { 'entities': undefined, }, // CASE NO entities
          { $and: [
              { 'entities.start_time': { $gte: new Date('Feburary 1, 2018 00:00:00') } },
              { 'entities.start_time': { $lt: new Date('March 1, 2018 00:00:00') } },
            ],
          },
        ],
      } },
      
      // REASSEMBLE topic value
      { $group: {
        _id: {
          _id: '$_id',
          count: {$sum: 1},
          entities: {
            _id: '$entities._id',
            start_time: '$entities.start_time',
            topic: '$entities.value',
          },
        },
        value: {
          count:{ $sum: "$entities.value"  },
        },
      } },
      // FIX EMPTY value
      { $project: {
        value: {
          $cond: {
            if: {
              $eq: [ '$value', null ],
            },
            then: [],
            else: [ '$value' ],
          },
        },
      } }, 
      // REASSEMBLE entities
      { $group: {
        _id: '$_id._id',
        name: { $first: '$_id.name' },
        entities: {
          $push: {
            _id: '$_id.entities._id',
            start_time: '$_id.entities.start_time',
            value: '$value',
          },
        },
      } },
      // FIX EMPTY entities
      { $project: {
        name: true,
        entities: {
          $cond: {
            if: {
              $eq: [ '$entities', [{ value: [] }] ],
            },
            then: [],
            else: '$entities',
          },
        },
      } }, 
    ]).toArray())
    .then(results => {
      console.log(inspect(results, false, null));
    })
    .then(() => client.close());
});