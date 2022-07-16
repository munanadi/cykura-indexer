/**
 * Script to populate daily volume tables for pools and tokens
 *
 * - Need to have tables in postgres
 * - This will populate tables for a given period
 *
 * - Will start from one day before end date given
 */

import { CyclosCore, IDL } from '@cykura/sdk';
import { Program, Wallet } from '@project-serum/anchor';
import { buildAnchorProvider } from '@saberhq/anchor-contrib';
import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import { Pool as pgPool } from 'pg';

async function populateVolumeTables(start: Date, end: Date) {
  const pool = new pgPool({
    connectionString: 'postgres://postgres:root@localhost:5432/postgres',
  });

  const PROGRAM_ADD = new PublicKey(
    'cysPXAjehMpVKUapzbMCCnpFxUFFryEWEaLgnb9NrR8'
  );

  const connection = new Connection('RPC_STRING');
  const wallet = new Wallet(Keypair.generate());

  const anchorProvider = buildAnchorProvider(connection, wallet, {
    commitment: 'finalized',
  });
  const cyclosCore = new Program<CyclosCore>(IDL, PROGRAM_ADD, anchorProvider);

  console.log(
    `Populating daily volume tables for pool and tokens between ${start.toString()} and ${end.toString()}`
  );

  // Fetch poolAddr and token mints that belong to it too.
  const poolsData = await cyclosCore.account.poolState.all();

  const poolTokensMap: {
    [poolAddr: string]: { token0Addr: string; token1Addr: string };
  } = {};

  for (const poolData of poolsData) {
    const { token0, token1 } = poolData.account;
    const poolAddr = poolData.publicKey.toString();

    poolTokensMap[poolAddr] = {
      token0Addr: token0.toString(),
      token1Addr: token1.toString(),
    };
  }

  // Starts from one day before end 00:00 to one day before that
  let yesterday = new Date();
  yesterday.setDate(end.getDate() - 1);
  yesterday.setHours(0, 0, 0);

  while (yesterday.getTime() > start.getTime()) {
    // Keep track of token daily token volumes
    const tokenVolumes: {
      [tokenAddr: string]: { date: number; volume: number };
    } = {};

    const dayBefore = new Date();
    dayBefore.setDate(yesterday.getDate() - 1);
    dayBefore.setHours(0, 0, 0);

    console.log(`Doing ${dayBefore.toString()} - ${yesterday.toString()} `);

    // Fetch Pool Volumes
    const data = await pool.query(
      'select * from good_txns where txn_blocktime between $1 and $2;',
      [dayBefore.getTime() / 1000, yesterday.getTime() / 1000]
    );

    // To track pool volumes
    const poolVolumes: {
      [poolAdd: string]: { amount0: number; amount1: number };
    } = {};

    for (const row of data.rows) {
      const { amount0, amount1, pool_addr: poolAddr } = row;

      if (!poolVolumes[poolAddr]) {
        // Init with values
        poolVolumes[poolAddr] = {
          amount0: Math.abs(+amount0),
          amount1: Math.abs(+amount1),
        };
      } else {
        // udpate the volume map
        poolVolumes[poolAddr]['amount0'] += Math.abs(amount0);
        poolVolumes[poolAddr]['amount1'] += Math.abs(amount1);
      }
    }

    let poolQuery = `INSERT INTO pool_volume_data(date, pool_addr, volume0, volume1) VALUES`;
    let tokenQuery = `INSERT INTO token_volume_data(date, token_addr, volume) VALUES`;

    // Flag to skip inserts if no data for a particular day
    const isDataPresent = Object.keys(poolVolumes).length > 0 ? true : false;

    for (const poolAddr of Object.keys(poolVolumes)) {
      const { amount0, amount1 } = poolVolumes[poolAddr];

      const { token0Addr, token1Addr } = poolTokensMap[poolAddr];

      // accumulate tokens volumes
      if (!tokenVolumes[token0Addr]) {
        // Init the value first
        tokenVolumes[token0Addr] = {
          date: yesterday.getTime(),
          volume: +amount0,
        };
      } else {
        tokenVolumes[token0Addr].volume += amount0;
      }

      if (!tokenVolumes[token1Addr]) {
        // Init the value first
        tokenVolumes[token1Addr] = {
          date: yesterday.getTime(),
          volume: +amount1,
        };
      } else {
        tokenVolumes[token1Addr].volume += amount1;
      }
    }

    poolQuery += `${Object.keys(poolVolumes).map(
      (poolAddr) =>
        `(${yesterday.getTime()}, '${poolAddr}', ${
          poolVolumes[poolAddr].amount0
        }, ${poolVolumes[poolAddr].amount1})`
    )}`;

    // Construct the token volume query
    tokenQuery += `${Object.keys(tokenVolumes).map(
      (tokenAddr) =>
        `(${yesterday.getTime()}, '${tokenAddr}' ,${
          tokenVolumes[tokenAddr].volume
        })`
    )}`;

    poolQuery += ' ON CONFLICT DO NOTHING;';
    tokenQuery += ' ON CONFLICT DO NOTHING;';

    // Insert records
    try {
      if (!isDataPresent) {
        console.log('No data found for this period');
        continue;
      }

      const poolRes = await pool.query(poolQuery);
      const tokenRes = await pool.query(tokenQuery);

      console.log(
        `${poolRes.rowCount} for pools and ${tokenRes.rowCount} for tokens inserted`
      );
    } catch (e) {
      console.log(e);
    } finally {
      // Next pair of days
      yesterday = new Date(dayBefore.getTime());
    }
  }
}

const startDate = new Date();
const endDate = new Date();

populateVolumeTables(endDate, startDate)
  .then(() => {
    console.log('DONE');
    process.exit(0);
  })
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
