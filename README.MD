# Archived Repository

This repository has been archived and is no longer maintained.

The calculator in this repository served as a first proof-of-concept
consensus layer reward calculator for the more technical audience
that was willing to run this script against their own consensus layer
nodes.
 
It has since evolved into a much more user-friendly way of accessing
this data, namely through the [ethstaker.tax](https://ethstaker.tax)
website. The website also has support for protocol upgrades such as
execution layer rewards and withdrawals.
 

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

- retrieves balance data from an Ethereum consensus layer node, using the
 parameters specified in [config.yml](config.yml).
- pulls corresponding price data from CoinGecko
- combines and writes these into a CSV file called
 `rewards_{VALIDATOR_INDEX}_{ETH2_ADDRESS}.csv`
