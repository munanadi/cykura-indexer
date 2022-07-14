#### Indexer for Cykura

Listens to swap events and adds them to a local postgres.

> This will start with the latest 1000 txns and continue fetching there on. Modifications to this can be done to populate the same database if required.

##### prerequisite
- Have postgres installed locally with postgres user
- `good_txns`table exists with the columns required

##### TODO
- Need to see why connection fails? Line 42.
- Add env variables back
- Listen to other events, only listening to `SwapEvent`
- Docekrize the whole thing?