# ETH2 Staking rewards calculator

Allows you to calculate the rewards on a day-by-day basis
(useful for tax purposes).

The output is a CSV file containing the following data:

| Date       | End-of-day balance [ETH] | Income for date [ETH] | Price for date [EUR/ETH] | Income for date [EUR] |
|------------|--------------------------|-----------------------|--------------------------|-----------------------|
| 2020-12-01 | 32.007299849             | 0.007299848999998915  | 512.861535490493         | 3.743811766988183     |
| ...        |                          |                       |                          |                       |
| 2020-12-31 | 32.421139665             | 0.010904340999999818  | 612.095211614956         | 6.67449491191653      |
| Total:     |                          | 0.42113966499999833   |                          | 212.81565225637513    | 

The currency can be changed in the [config file](config.yml) .

## Requirements
- Python 3.6+
- installed Python dependencies (`pip install -r requirements.txt`)

## Usage
`python3 main.py`

The script:

- retrieves balance data from an ETH2 beacon node, using the
 parameters specified in [config.yml](config.yml) .
 If you're running a Prysm beacon node, make sure
 to set the `PRYSM_API` config option to True.
 If you're running a Nimbus beacon node, make sure
 to set the `NIMBUS_API` config option to True.
- pulls corresponding price data from CoinGecko
- combines and writes these into a CSV file called `rewards_{VALIDATOR_INDEX}.csv` 

## Tested with

- [x] Lighthouse v1.1.0
- [x] Prysm v1.1.0 (set `PRYSM_API` to True in the config file)
- [x] Teku v21.1.1
  - Teku prunes chain data by default - if you're getting 404 errors,
  make sure you are running Teku in
  [archive](https://docs.teku.consensys.net/en/latest/Reference/CLI/CLI-Syntax/#data-storage-mode)
  mode to be able to retrieve balances for finalized epochs (`--data-storage-mode=archive`)
- [x] Nimbus v1.0.7 (set `NIMBUS_API` to True in the config file)

## Donation

If this saved you some time, buy me a 🍺!

![🍺](donate.png?raw=true)
