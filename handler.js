'use strict';
const https = require('https');
const AWS = require('aws-sdk')
const { Pool } = require('pg');


module.exports.run = async (event, context) => {
  const pool = new Pool({
    user: 'admin',
    host: 'redshift-cluster-1.cn4rvtbrptq4.us-east-2.redshift.amazonaws.com',
    database: 'dev',
    password: 'LuckyRedshift777!',
    port: 5439,
  });
  const now = await pool.query('SELECT * FROM "dev"."public"."fact_slot_players_flagged" order by slot_machine_id');

  const now_dev = await pool.query('SELECT * FROM "dev"."public"."fact_slot_players_flagged_dev" order by slot_machine_id');




  await doPostRequest(JSON.stringify(now.rows))
    .then(result => console.log(`Status code: ${result}`))
    .catch(err => console.error(`Error doing the request for the event: ${JSON.stringify(event)} => ${err}`));
  // Dev Table to dev api
  await doPostRequestDev(JSON.stringify(now_dev.rows),"fact_slot_players_flagged_dev")
    .then(result => console.log(`Status code dev: ${result}`))
    .catch(err => console.error(`Error doing the request for the event: ${JSON.stringify(event)} => ${err}`));
  // Main Table to dev api
  await doPostRequestDev(JSON.stringify(now.rows))
    .then(result => console.log(`Status code dev: ${result}`))
    .catch(err => console.error(`Error doing the request for the event: ${JSON.stringify(event)} => ${err}`));

  //const dev_now = await pool.query('SELECT * FROM "dev"."public"."fact_slot_players_flagged"');
  await pool.end();



};

const doPostRequest = (playersFlagged) => {
  const data = {
    "expiryInSeconds": -1,
    "k": "player_list",
    "tableName": "fact_slot_players_flagged",
    "v": playersFlagged
  };

  return new Promise((resolve, reject) => {
    const options = {
      host: 'api.luckyvr.net',
      path: '/ss/set',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    };


    const req = https.request(options, (res) => {

      resolve(JSON.stringify(res.statusCode));
    });


    req.on('error', (e) => {
      reject(e.message);
    });


    req.write(JSON.stringify(data));


    req.end();
  });
};


const doPostRequestDev = (playersFlagged, table_name = "fact_slot_players_flagged") => {

  const data = {
    "expiryInSeconds": -1,
    "k": "player_list",
    "tableName": table_name,
    "v": playersFlagged
  };

  return new Promise((resolve, reject) => {
    const options = {
      host: 'api-dev.luckyvr.net',
      path: '/ss/set',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    };


    const req = https.request(options, (res) => {
      console.log(res.statusCode, table_name)
      resolve(JSON.stringify(res.statusCode));
    });


    req.on('error', (e) => {
      reject(e.message);
    });


    req.write(JSON.stringify(data));


    req.end();
  });
};
