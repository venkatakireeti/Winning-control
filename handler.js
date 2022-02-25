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
  await doPostRequestDev(JSON.stringify(now_dev.rows), "fact_slot_players_flagged_dev")
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


module.exports.slotRun = async (event, context) => {
  const pool = new Pool({
    user: 'admin',
    host: 'redshift-cluster-1.cn4rvtbrptq4.us-east-2.redshift.amazonaws.com',
    database: 'dev',
    password: 'LuckyRedshift777!',
    port: 5439,
  });
  const RTP_MAX = 2;
  const START_DATE = new Date();
  const END_DATE = new Date(new Date().setMinutes(new Date().getMinutes() - 30));
  //const END_DATE = new Date(new Date().setHours(new Date().getHours() - 1));


  // Query that looks at stage live 
  const now = await pool.query(`select * from(select PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rtp) as rtp, COUNT(user_name) as spin, machine_id,user_name,user_id from (select ( case when total = 0 then 0 else (CAST(total AS FLOAT)/CAST( (case when totalbet = 0 then 1 else totalbet end) AS FLOAT)) end) as rtp, machine_id, user_name,user_id from (select (totalwinnings+jackpotwins) as total, totalbet, machine_id,user_name,user_id from (select distinct "dev"."public"."fact_slot_machine_spin".spin_id, machine_id,user_id ,user_name,( case when win_amount is null then 0 else win_amount end) as JackpotWins,winnings as totalwinnings,( case when was_free_game = true then 0 else bet end) as TotalBet from "dev"."public"."fact_slot_machine_spin" join "dev"."public"."dim_user" as dim_user on user_name=SUBSTRING(REPLACE(spin_id,'.','_'),1,CHARINDEX('-',spin_id)-1) full outer join "dev"."public"."fact_slot_jackpot" as jackpot  on "dev"."public"."fact_slot_machine_spin".spin_id = jackpot.spin_id where "dev"."public"."fact_slot_machine_spin".time >  ${END_DATE.getTime()} AND "dev"."public"."fact_slot_machine_spin".time < ${START_DATE.getTime()} AND stage = 'live' AND in_tournament_mode = false ))) group by user_name,machine_id,user_id ORDER BY user_name) where rtp > 2`);
  //TODO - new query that looks at stage preview
  const dev_now = await pool.query(`select * from(select PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rtp) as rtp, COUNT(user_name) as spin, machine_id,user_name,user_id from (select ( case when total = 0 then 0 else (CAST(total AS FLOAT)/CAST( (case when totalbet = 0 then 1 else totalbet end) AS FLOAT)) end) as rtp, machine_id, user_name,user_id from (select (totalwinnings+jackpotwins) as total, totalbet, machine_id,user_name,user_id from (select distinct "dev"."public"."fact_slot_machine_spin".spin_id, machine_id,user_id ,user_name,( case when win_amount is null then 0 else win_amount end) as JackpotWins,winnings as totalwinnings,( case when was_free_game = true then 0 else bet end) as TotalBet from "dev"."public"."fact_slot_machine_spin" join "dev"."public"."dim_user" as dim_user on user_name=SUBSTRING(REPLACE(spin_id,'.','_'),1,CHARINDEX('-',spin_id)-1) full outer join "dev"."public"."fact_slot_jackpot" as jackpot  on "dev"."public"."fact_slot_machine_spin".spin_id = jackpot.spin_id where "dev"."public"."fact_slot_machine_spin".time >  ${END_DATE.getTime()} AND "dev"."public"."fact_slot_machine_spin".time < ${START_DATE.getTime()} AND stage = 'preview' AND in_tournament_mode = false ))) group by user_name,machine_id,user_id ORDER BY user_name) where rtp > 2`);

  // Push rtp update into fact_slot_machine_rtp
  // TODO: Include stage when pushing data to slot_machine_rtp
  let insertIntoLive = 'INSERT INTO "dev"."public"."fact_slot_players_flagged" (username,date_of_flag,slot_machine_id,median_rtp,number_of_spins,user_id) VALUES (\'' + now.rows[0].user_name + '\',' + (new Date()).getTime() + ',\'' + now.rows[0].machine_id + '\',' + now.rows[0].rtp + ',' + now.rows[0].spin + ',\'' + now.rows[0].user_id + '\')'

  for (let i = 1; i < now.rows.length; i++) {
    insertIntoLive += ',(\'' + now.rows[i].user_name + '\',' + (new Date()).getTime() + ',\'' + now.rows[i].machine_id + '\',' + now.rows[i].rtp + ',' + now.rows[i].spin + ',\'' + now.rows[i].user_id + '\')';

  }

  await pool.query(insertIntoLive);

  // Change machine status flase in dev
  if (dev_now.rows.length > 0) {
    let insertIntoDevQuery = 'INSERT INTO "dev"."public"."fact_slot_players_flagged_dev" (username,date_of_flag,slot_machine_id,median_rtp,number_of_spins,user_id) VALUES (\'' + now.rows[0].user_name + '\',' + (new Date()).getTime() + ',\'' + now.rows[0].machine_id + '\',' + now.rows[0].rtp + ',' + now.rows[0].spin + ',\'' + now.rows[0].user_id + '\')';
    for (let i = 1; i < dev_now.rows.length; i++) {
      insertIntoDevQuery += ',(\'' + now.rows[i].user_name + '\',' + (new Date()).getTime() + ',\'' + now.rows[i].machine_id + '\',' + now.rows[i].rtp + ',' + now.rows[i].spin + ',\'' + now.rows[i].user_id + '\')';

    }
    await pool.query(insertIntoDevQuery);
  }

  await pool.end();
}