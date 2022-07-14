import {
  Connection,
  ParsedTransactionWithMeta,
  PublicKey,
} from '@solana/web3.js';
import { CyclosCore as CykuraCore, IDL } from '@cykura/sdk';
import { buildCoderMap, AnchorTypes } from '@saberhq/anchor-contrib';
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: 'postgres://ubuntu:root@localhost:5432/postgres',
});

const PROGRAM_ADD = new PublicKey(
  'cysPXAjehMpVKUapzbMCCnpFxUFFryEWEaLgnb9NrR8'
);

const cykuraBuilder = buildCoderMap(
  { cykura: IDL },
  {
    cykura: PROGRAM_ADD,
  }
);

type Events = CykuraTypes['Events'];
type SwapEvent = Events['SwapEvent'];

type CykuraTypes = AnchorTypes<
  CykuraCore,
  {},
  {},
  {
    swap: SwapEvent;
  }
>;

// Store the before hash if need to restart fetching from some point
let largestBlockTime = 0;
let largestTxnHash: string;

// const wallet = new Wallet(Keypair.generate());
const connection = new Connection(
  '<RPC_STRING_GOES_HERE>'
);

const txnHashRefetchSet = new Set<string>();

const txnHashCacheSet = new Set<string>();

(async function () {
  await fetchTxns();
})();

async function fetchTxns(fetchBeforeTxn?: string): Promise<any> {
  // Clean up - clear sets if they go above 1000
  if (txnHashCacheSet.size >= 1000) {
    txnHashCacheSet.clear();
  }
  if (txnHashRefetchSet.size >= 1000) {
    txnHashRefetchSet.clear();
  }

  // Fetch until given hash or the latest 1000 txns
  const data = fetchBeforeTxn
    ? await connection.getConfirmedSignaturesForAddress2(
        PROGRAM_ADD,
        {
          until: fetchBeforeTxn,
        },
        'finalized'
      )
    : await connection.getConfirmedSignaturesForAddress2(
        PROGRAM_ADD,
        {},
        'finalized'
      );

  if (fetchBeforeTxn) {
    console.log(`Fetching until ${fetchBeforeTxn}`);
  } else {
    console.log('Fetching latest 1000 txns');
  }

  let prevLargestFetched = largestTxnHash;

  /// filter out the error ones
  const signArr = data
    .filter((d) => {
      // init condition
      if (largestBlockTime == 0) {
        largestBlockTime = d.blockTime ?? 0;
        largestTxnHash = d.signature;
      }
      // TODO: What if I don't get a blocktime here?
      else if (d.blockTime && largestBlockTime < d.blockTime) {
        largestBlockTime = d.blockTime;
        largestTxnHash = d.signature;
      }

      return !d.err;
    })
    .map((d) => {
      // cache already fetched txns, don't refetch them again
      if (txnHashCacheSet.has(d.signature)) {
        return '';
      }
      txnHashCacheSet.add(d.signature);
      return d.signature;
    })
    .filter((t) => t !== ''); // To filter hash from cache that are already fetched

  console.log(`Got ${signArr.length} new signatures to fetch`);

  // If no new transactions are there to fetch
  if (signArr.length === 0) {
    console.log('Ran out of signatures.');
    console.log(
      `The largest blocktime is ${new Date(
        largestBlockTime * 1000
      ).toString()} with hash ${largestTxnHash}`
    );
    return setTimeout(async () => {
      await fetchTxns(largestTxnHash);
    }, 5000);
  }

  let fetchTxnSign = signArr;

  console.log(
    `Fetching ${fetchTxnSign.length} signatures and ${txnHashRefetchSet.size} unfetched ones from previous run`
  );

  let txnArr: (ParsedTransactionWithMeta | null)[];

  // Add the unfetched ones from previous iteration
  fetchTxnSign.concat(Array.from(txnHashRefetchSet));

  try {
    txnArr = await connection.getParsedTransactions(fetchTxnSign);
  } catch (e) {
    // TODO: with what txn hash do I call this thing?
    txnArr = [];
    console.log(
      'Failed to fetch parsed transactions for signatures, refetching from previous'
    );
    fetchTxns(prevLargestFetched);
  }

  // TODO: What txn hash do I call this with?
  if (!txnArr.length) {
    console.log('txnArr is null');
    return fetchTxns(prevLargestFetched);
  }

  for (let i = 0; i < fetchTxnSign.length; i++) {
    if (!txnArr[i]) {
      txnHashRefetchSet.add(fetchTxnSign[i]);
      // console.log(`Couldn't fetch for ${fetchTxnSign[i]} `);
    }
    if (txnHashRefetchSet.has(fetchTxnSign[i])) {
      if (txnArr[i]) {
        // Fetched remove from hash set
        txnHashRefetchSet.delete(fetchTxnSign[i]);
      }
    }
  }

  // console.log('Adding Txns');
  let count = 0;

  // Filter nulls from txnArr
  txnArr = txnArr.filter((t) => t);

  const values: Array<Array<string>> = [];

  for (const txn of txnArr) {
    if (!txn) {
      // THIS SHOULD NOT HAPPEN
      console.log('NULL TXNS');
      continue;
    }

    const txnHash = txn.transaction.signatures[0];
    const txnBlockTime = txn.blockTime ?? 0;
    const txnLogs = txn.meta?.logMessages ?? [];

    let events: any;
    try {
      events = cykuraBuilder.cykura.parseProgramLogEvents<any>(txnLogs);
    } catch (e) {
      // Handle things like
      // 1. upgrade of contracts = "2jAwQP4HgfRkD7M5ry6HVuHiqPstgXaULfQqhJMbFEaXcZqnyWQaLCcKv5xvgmQEWaso1RQGjscmCD4CSnVEAv5z"
      console.log('parseProgramLogEvents fails @ ', txnHash);
      console.log(txn?.meta?.logMessages);
      return fetchTxns(txnHash);
    }

    for (const e of events) {
      if (e.name === 'SwapEvent') {
        const data = e.data;

        console.log(
          ` - ${txnHash} ${new Date(txnBlockTime * 1000)
            .toLocaleString('en-Us', { timeZone: 'Asia/Kolkata' })
            .slice()} ${data.poolState.toString()} ${data.amount0.toString()} ${data.amount1.toString()}`
        );

        // Just to keep count
        count++;

        values.push([
          txnHash.toString(),
          txnBlockTime.toString(),
          data.poolState.toString(),
          data.sender.toString(),
          data.amount0.toString(),
          data.amount1.toString(),
        ]);
      } else {
        console.log(
          'Something else comes here',
          e.name ?? "Don't  know events name"
        );
      }
    }
  }

  // Add to postgres locally if any records present
  if (values.length !== 0) {
    let query = `INSERT INTO good_txns(txn_hash,txn_blocktime,pool_addr,sender,amount0,amount1) VALUES ${values.map(
      (value) => `(${value.map((r) => `'${r.toString()}'`)})`
    )}`;
    query += 'ON CONFLICT DO NOTHING ;';

    pool.query(query, (err, res) => {
      console.log('Found', count, 'swaps');
      if (err) {
        console.log(err);
      } else {
        console.log(res.rowCount, ' added');
      }
    });
  }

  console.log(
    `The largest hash in the batch is ${largestTxnHash}  @ ${new Date(
      largestBlockTime * 1000
    ).toLocaleString('en-Us', { timeZone: 'Asia/Kolkata' })}`
  );

  return fetchTxns(largestTxnHash);
}

