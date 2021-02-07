import csv
import datetime
import json
import logging
import math
import sys
from collections import namedtuple
from multiprocessing.pool import ThreadPool
from typing import List

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry
from yaml import safe_load

DataPoint = namedtuple("DataPoint", ["validator_index", "datetime", "balance"])
SLOT_TIME = 12
SLOTS_IN_EPOCH = 32
GENESIS_DATETIME = datetime.datetime.fromtimestamp(1606824023)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout
)

logger = logging.getLogger()

with open("config.yml") as f:
    config = safe_load(f)

# Retry all HTTP requests a few times with exponential backoff
s = requests.Session()

retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[413, 429, 500, 503],
    raise_on_status=False,
)

s.mount("http://", HTTPAdapter(max_retries=retries))


def get_datapoints_for_slot(slot: int) -> List[DataPoint]:
    """Returns DataPoint objects for the specified slot."""
    validator_indexes = config["VALIDATOR_INDEXES"]

    host = config["BEACON_NODE"]["HOST"]
    port = config["BEACON_NODE"]["PORT"]
    prysm_api = config["BEACON_NODE"]["PRYSM_API"]
    nimbus_api = config["BEACON_NODE"]["NIMBUS_API"]
    if prysm_api and nimbus_api:
        raise Exception(f"PRYSM_API and NIMBUS_API cannot both be set to True")

    slot_datetime = GENESIS_DATETIME + datetime.timedelta(seconds=slot * SLOT_TIME)

    logger.debug(f"Getting data for slot {slot}")
    if prysm_api:
        epoch_for_slot = math.floor(slot / SLOTS_IN_EPOCH)

        resp = s.get(
            f"http://{host}:{port}/eth/v1alpha1/validators/balances",
            params={"epoch": epoch_for_slot, "indices": [validator_indexes]},
        )
        data = resp.json()

        if resp.status_code != 200:
            raise Exception(
                f"Error while fetching data from beacon node: {resp.content.decode()}"
            )

        datapoints = []
        for d in data["balances"]:
            datapoints.append(
                DataPoint(
                    validator_index=int(d["index"]),
                    datetime=slot_datetime,
                    balance=int(d["balance"]) / 1000000000,
                )
            )

        while data["nextPageToken"] != "":
            resp = s.get(
                f"http://{host}:{port}/eth/v1alpha1/validators/balances",
                params={
                    "epoch": epoch_for_slot,
                    "indices": [validator_indexes],
                    "page_token": data["nextPageToken"],
                },
            )
            data = resp.json()
            for d in data["balances"]:
                datapoints.append(
                    DataPoint(
                        validator_index=int(d["index"]),
                        datetime=slot_datetime,
                        balance=int(d["balance"]) / 1000000000,
                    )
                )
    elif nimbus_api:
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "get_v1_beacon_states_stateId_validator_balances",
                "params": [str(slot)],
                "id": validator_indexes,
            }
        )
        headers = {"content-type": "application/json"}
        resp = s.post(f"http://{host}:{port}/", data=payload, headers=headers)

        if resp.status_code != 200:
            raise Exception(
                f"Error while fetching data from beacon node: {resp.content.decode()}"
            )

        data = resp.json()

        if data["error"]:
            raise Exception(
                f"Error while fetching data from beacon node: {data['error']}"
            )

        data = [d for d in data["result"] if int(d["index"]) in validator_indexes]

        datapoints = []
        for d in data:
            datapoints.append(
                DataPoint(
                    validator_index=int(d["index"]),
                    datetime=slot_datetime,
                    balance=int(d["balance"]) / 1000000000,
                )
            )
    else:
        resp = s.get(
            f"http://{host}:{port}/eth/v1/beacon/states/{slot}/validator_balances"
        )

        if resp.status_code != 200:
            raise Exception(
                f"Error while fetching data from beacon node: {resp.content.decode()}"
            )

        data = resp.json()["data"]

        data = [d for d in data if int(d["index"]) in validator_indexes]

        datapoints = []
        for d in data:
            datapoints.append(
                DataPoint(
                    validator_index=int(d["index"]),
                    datetime=slot_datetime,
                    balance=int(d["balance"]) / 1000000000,
                )
            )
    return datapoints


def slot_no_for_datetime(dt: datetime.datetime) -> int:
    return math.floor((dt - GENESIS_DATETIME).total_seconds() / SLOT_TIME)


def get_all_datapoints() -> List[DataPoint]:
    """
    Retrieves rewards data for all validator indexes from the beacon node.

    Returns:
        A list of DataPoint objects.
    """
    start_date = datetime.datetime.fromisoformat(config["START_DATE"])
    start_date = max(start_date, GENESIS_DATETIME)
    end_date = datetime.datetime.fromisoformat(config["END_DATE"]) + datetime.timedelta(
        days=1
    )
    end_date = min(end_date, datetime.datetime.now())

    # Calculate the slot numbers at which we need to retrieve the balance
    slot_numbers = []
    # Calculate the initial balance at the start of START_DATE
    initial_slot_no = slot_no_for_datetime(start_date)
    slot_numbers.append(initial_slot_no)

    current_date = start_date
    while current_date < end_date:
        last_slot_datetime = current_date.replace(hour=23, minute=59, second=59)
        last_slot_datetime = min(last_slot_datetime, datetime.datetime.now())

        last_slot_no = slot_no_for_datetime(last_slot_datetime)
        slot_numbers.append(last_slot_no)

        # Move on to the next day
        current_date = current_date + datetime.timedelta(days=1)

    logger.debug(
        f"Calculating rewards for slots {min(slot_numbers)} - {max(slot_numbers)}"
    )

    datapoints = []
    with ThreadPool(config["BEACON_NODE"]["CONCURRENT_REQUESTS"]) as p:
        for dp in tqdm(
            p.imap(get_datapoints_for_slot, slot_numbers),
            desc=f"Retrieving beacon chain data...",
            total=len(slot_numbers),
        ):
            datapoints.extend(dp)

    return datapoints


def write_rewards_to_file(datapoints: List[DataPoint]):
    """
    Processes the datapoints, retrieves corresponding price data
    and writes it to a file.
    """
    currency = config["CURRENCY"]
    validator_indexes = config["VALIDATOR_INDEXES"]

    eth_price = {}
    for dp in tqdm(datapoints, desc="Fetching price data from CoinGecko"):
        try:
            _ = eth_price[dp.datetime.strftime(format="%d-%m-%Y")]
            # Price already known for this date
            continue
        except KeyError:
            try:
                resp = s.get(
                    f"https://api.coingecko.com/api/v3/coins/ethereum/history",
                    params={"date": dp.datetime.strftime(format="%d-%m-%Y")},
                )
                cg_data = resp.json()
            except Exception as e:
                logger.error(
                    "Something went wrong while fetching price data from CoinGecko"
                )
                raise e
            eth_price[dp.datetime.strftime(format="%d-%m-%Y")] = cg_data["market_data"][
                "current_price"
            ][currency.lower()]

    for validator_index in tqdm(validator_indexes, desc="Writing rewards to file"):
        with open(f"rewards_{validator_index}.csv", "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            writer.writerow(
                [
                    "Date",
                    "End-of-day balance [ETH]",
                    "Income for date [ETH]",
                    f"Price for date [{currency}/ETH]",
                    f"Income for date [{currency}]",
                ]
            )

            validator_datapoints = [
                dp for dp in datapoints if dp.validator_index == validator_index
            ]

            prev_balance = validator_datapoints[0].balance
            total_income_eth = 0
            total_income_curr = 0

            for dp in validator_datapoints[1:]:
                price_for_date_curr = eth_price[dp.datetime.strftime(format="%d-%m-%Y")]

                income_for_date_eth = dp.balance - prev_balance
                total_income_eth += income_for_date_eth

                income_for_date_curr = price_for_date_curr * income_for_date_eth
                total_income_curr += income_for_date_curr

                writer.writerow(
                    [
                        dp.datetime.strftime(format="%Y-%m-%d"),
                        dp.balance,
                        income_for_date_eth,
                        price_for_date_curr,
                        income_for_date_curr,
                    ]
                )
                prev_balance = dp.balance
            writer.writerow(["Total:", "", total_income_eth, "", total_income_curr])
    logger.info("All done!")


def main():
    datapoints = get_all_datapoints()
    write_rewards_to_file(datapoints=datapoints)


if __name__ == "__main__":
    main()
