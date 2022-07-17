/**
 * Script to populate Event tables
 *
 * - Need to have tables in postgres
 * - This will populate tables for a given period
 *
 * - Will start from one day before end date given
 */

import { IDL } from '@cykura/sdk';
import { buildCoderMap } from '@saberhq/anchor-contrib';
import { Connection, PublicKey } from '@solana/web3.js';
import { Pool as pgPool } from 'pg';

const pool = new pgPool({
  connectionString: 'postgres://postgres:root@localhost:5432/postgres',
});

const PROGRAM_ADD = new PublicKey(
  'cysPXAjehMpVKUapzbMCCnpFxUFFryEWEaLgnb9NrR8'
);

const connection = new Connection('RPC_STRING');
const cykuraBuilder = buildCoderMap(
  { cykura: IDL },
  {
    cykura: PROGRAM_ADD,
  }
);

let smallestBlockTime = 0;
let smallestTxnHash = '';

async function populateEventTables(fetchBeforeTxn: string): Promise<string> {
  console.log(`Fetching transactions before ${fetchBeforeTxn}`);

  /** Fetch the latest transcation by iteslf and continue the loop from there. */
  const data = await connection.getConfirmedSignaturesForAddress2(
    PROGRAM_ADD,
    {
      before: fetchBeforeTxn,
    },
    'finalized'
  );

  let prevSmallestFetched = fetchBeforeTxn;

  /// Store the smallest blocktime and its corresponding hash
  /// filter out the error ones
  const signArr = data
    .filter((d) => {
      if (smallestBlockTime == 0) {
        smallestTxnHash = d.signature;
        smallestBlockTime = d.blockTime!;
      }
      if (smallestBlockTime > d.blockTime!) {
        smallestBlockTime = d.blockTime!;
        smallestTxnHash = d.signature;
      }
      return !d.err;
    })
    .map((d) => d.signature);

  console.log(`Got ${signArr.length} signatures`);

  // TODO: Fix for when we actually reach the end of the progarm lifetime
  // This just keeps repeating the last txn hash ever and loops over itself.
  // How would I know if that was indeed the last txn hash that was there?
  if (signArr.length === 0) {
    console.log('Ran out of signatures.');
    console.log(
      `The smallest blocktime is ${new Date(
        smallestBlockTime * 1000
      ).toString()} with hash ${smallestTxnHash}`
    );
    return populateEventTables(smallestTxnHash);
  }

  let fetchTxnSigns = signArr;

  console.log(`Fetching ${fetchTxnSigns.length}`);

  let txnArr: any[] = [];
  try {
    txnArr = await connection.getParsedTransactions(fetchTxnSigns);
  } catch (e) {
    console.log(`Fetching transations failed`, e);
    populateEventTables(prevSmallestFetched);
  }

  if (!txnArr) {
    console.log('txnArr is null');
    return populateEventTables(prevSmallestFetched);
  }

  console.log('Adding Txns');
  let count = 0;

  let query = `INSERT INTO good_txns(txn_hash, txn_blocktime, pool_addr, sender, amount0, amount1) VALUES`;
  let eventsArr: {
    txnHash: string;
    txnBlockTime: number;
    sender: string;
    poolAddr: string;
    amount0: number;
    amount1: number;
  }[] = [];

  for (const txn of txnArr) {
    if (!txn) {
      // refetch all the tranasction from the top;
      // TODO: Need to be efficient and skip only the null ones here.
      //       This doesn't happen that often thoughs
      return populateEventTables(prevSmallestFetched);
    }

    const txnHash = txn.transaction.signatures[0];
    const txnBlockTime = txn.blockTime;
    const txnLogs = txn.meta?.logMessages ?? [];

    let events;
    try {
      events = cykuraBuilder.cykura.eventParser.parseLogs(txnLogs);
    } catch (e) {
      // Handle things like
      // 1. upgrade of contracts = "2jAwQP4HgfRkD7M5ry6HVuHiqPstgXaULfQqhJMbFEaXcZqnyWQaLCcKv5xvgmQEWaso1RQGjscmCD4CSnVEAv5z"
      console.log('parseProgramLogEvents fails @ ', txnHash);
      console.log(txn?.meta?.logMessages);
      return populateEventTables(txnHash);
    }

    // console.log(events);

    for (const e of events) {
      if (e.name === 'SwapEvent') {
        const data = e.data;

        console.log(
          ` - ${txnHash} ${new Date((txnBlockTime ?? 0) * 1000)
            .toLocaleString('en-Us', { timeZone: 'Asia/Kolkata' })
            .slice()} ${data.poolState.toString()} ${data.amount0.toString()} ${data.amount1.toString()}`
        );

        // Just to keep count
        count++;

        eventsArr.push({
          txnHash: txnHash,
          txnBlockTime: txnBlockTime!,
          sender: data.sender.toString(),
          poolAddr: data.poolState.toString(),
          amount0: data.amount0,
          amount1: data.amount1,
        });
      }
    }
  }

  console.log('Found', count, 'swaps');

  try {
    if (count == 0) {
      console.log(0, ' number of rows inserted');
    } else {
      query += `${eventsArr.map(
        (e) =>
          `('${e.txnHash}', ${e.txnBlockTime}, '${e.poolAddr}', '${e.sender}', ${e.amount0}, ${e.amount1})`
      )}`;

      query += ' ON CONFLICT DO NOTHING;';

      const res = await pool.query(query);

      console.log(res.rowCount, ' number of rows inserted');
    }
  } catch (e) {
    console.log('Something went wrong');
    console.log(e);
  } finally {
    console.log(
      `The smallest hash in the batch is ${smallestTxnHash}  @ ${new Date(
        smallestBlockTime * 1000
      ).toLocaleString('en-Us', { timeZone: 'Asia/Kolkata' })}`
    );

    return populateEventTables(smallestTxnHash);
  }
}

(async function () {
  await populateEventTables(
    'oqJ1YNQRijwpD7QxvXz9iRuD8eyAr2cuERbJEZi6y3PPJ3A4am82fCr7EXWtemYMReoHF8VEcrkuSQ7Yk96HcPN'
  );
})();
