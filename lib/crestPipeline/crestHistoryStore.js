var postgres = require('../postgres');
var eventCenter = require('../eventCenter');

function prepareQueryValues(resultSet) {
  //
  // Prepare parametrized query
  //

  var params = '';
  var values = [];
  var count = 1;

  resultSet.items.forEach(function(object){

    // Construct params string
    params += '($'+(count++)+
              '::int4, $'+(count++)+
              '::int4, $'+(count++)+
              '::int4, $'+(count++)+
              '::float8, $'+(count++)+
              '::float8, $'+(count++)+
              '::float8, $'+(count++)+
              '::int8, $'+(count++)+
              "::timestamp AT TIME ZONE 'UTC'),";

    // Add object's values to array
    values.push(resultSet.regionID,
                resultSet.typeID,
                object.orderCount,
                object.lowPrice,
                object.highPrice,
                object.avgPrice,
                object.volume,
                object.date);
  });

  // Cut last comma
  params = params.slice(0, -1);

  return {
      params: params,
      values: values
  };
}

function composeQuery(objects) {

  // Concatenate upsert query
  var query = 'WITH new_values (mapregion_id, invtype_id, numorders, low, high, mean, quantity, date) ' +
              'AS (VALUES ' + objects.params + '), ' +
              'upsert as ' +
              '(' +
                'UPDATE market_data_orderhistory o ' +
                  'SET numorders = new_value.numorders, ' +
                      'low = new_value.low, ' +
                      'high = new_value.high, ' +
                      'mean = new_value.mean, ' +
                      'quantity = new_value.quantity ' +
                  'FROM new_values new_value ' +
                  'WHERE o.mapregion_id = new_value.mapregion_id ' +
                    'AND o.invtype_id = new_value.invtype_id ' +
                    'AND o.date = new_value.date ' +
                    'AND o.date >= NOW() - \'1 day\'::INTERVAL ' +
                  'RETURNING o.*' +
              ') ' +

              'INSERT INTO market_data_orderhistory (mapregion_id, invtype_id, numorders, low, high, mean, quantity, date) ' +
                'SELECT mapregion_id, invtype_id, numorders, low, high, mean, quantity, date ' +
                'FROM new_values ' +
                'WHERE NOT EXISTS (SELECT 1 ' +
                                  'FROM upsert up ' +
                                  'WHERE up.mapregion_id = new_values.mapregion_id ' +
                                    'AND up.invtype_id = new_values.invtype_id ' +
                                    'AND up.date = new_values.date) ' +

                'AND NOT EXISTS (SELECT 1 FROM market_data_orderhistory WHERE mapregion_id = new_values.mapregion_id ' +
                                                                         'AND invtype_id = new_values.invtype_id ' +
                                                                         'AND date = new_values.date)';

  return query;
}


exports = module.exports = function(resultSet, callback) {
  //
  // Stores history data
  //

  // Prepare parametrized format
  var objects = prepareQueryValues(resultSet);

  // Compose query
  var query = composeQuery(objects);

  // Get connection from pool
  postgres(function(err, pgClient, done) {
    if (err) {
        // Return connection to pool
        done();

        err.module = 'crestHistoryStore';

        // Handle errors
        return callback(err, null);
    } else {
      // Execute query
      pgClient.query(query, objects.values, function(err, result) {

        // Return connection to pool
        done();

        // Handle errors
        if(err && err.code === '23503') {
          console.log('SDE Outdated: '.yellow + err.detail);
        } else if (err) {
          err.module = 'crestHistoryStoreUpdate';
          return callback(err, null);
        } else {
          // Fire event
          eventCenter.emit('updatedHistory', objects.values.length - result.rowCount);
        }
      });
    }
  });

  return callback(null, resultSet);

};