import codecs
import csv
import datetime
import json
import logging
import math
import sys
from base64 import b64encode, b64decode
from collections import namedtuple
from itertools import filterfalse
from multiprocessing.pool import ThreadPool
from typing import List
import re

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry
from yaml import safe_load
import pytz

DataPoint = namedtuple("DataPoint", ["validator_index", "datetime", "balance"])
Validator = namedtuple("Validator", ["validator_index", "eth2_address"])
SLOT_TIME = 12
SLOTS_IN_EPOCH = 32
UTC_TIMEZONE = pytz.utc
GENESIS_DATETIME = UTC_TIMEZONE.localize(datetime.datetime.utcfromtimestamp(1606824023))

with open("config.yml") as f:
    config = safe_load(f)

logging.basicConfig(
    level=config["LOG_LEVEL"], format="%(asctime)s %(message)s", stream=sys.stdout
)

logger = logging.getLogger()

# Retry all HTTP requests a few times with exponential backoff
s = requests.Session()

retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[413, 429, 500, 503],
    raise_on_status=False,
)

s.mount("http://", HTTPAdapter(max_retries=retries))
s.mount("https://", HTTPAdapter(max_retries=retries))

host = config["BEACON_NODE"]["HOST"]
port = config["BEACON_NODE"]["PORT"]
validators = []


def get_validator(idx: int = None, addr: str = None) -> Validator:
    if idx and addr:
        raise ValueError("Both idx and addr provided to get_validator")
    elif not (idx or addr):
        raise ValueError("None of idx or addr provided to get_validator")

    if addr:
        resp = s.get(
            f"http://{host}:{port}/eth/v1/beacon/states/head/validators/{addr}"
        )
    else:
        resp = s.get(
            f"http://{host}:{port}/eth/v1/beacon/states/head/validators/{idx}"
        )

    if resp.status_code != 200:
        raise Exception(
            f"Error while fetching data from beacon node: {resp.content.decode()}"
        )

    data = resp.json()["data"]
    return Validator(
        validator_index=int(data["index"]), eth2_address=data["validator"]["pubkey"]
    )


def get_validators_for_eth1_address(eth1_address: str) -> List[Validator]:
    resp = s.get(f"https://beaconcha.in/api/v1/validator/eth1/{eth1_address}")

    if resp.status_code != 200:
        raise Exception(
            f"Failed to retrieve validators for ETH1 address {eth1_address} - {resp.content.decode()}"
        )

    data = resp.json()["data"]

    vals_for_addr = []
    for validator_data in data:
        vals_for_addr.append(
            Validator(
                validator_index=int(validator_data["validatorindex"]),
                eth2_address=validator_data["publickey"],
            )
        )

    return vals_for_addr


def get_validators() -> List[Validator]:
    validators = set()
    if type(config.get("VALIDATOR_INDEXES")) == list:
        for index in config["VALIDATOR_INDEXES"]:
            validator = get_validator(idx=index)
            logger.info(f"Added validator from VALIDATOR_INDEXES - {validator}")
            validators.add(validator)

    if type(config.get("ETH2_ADDRESSES")) == list:
        for address in config["ETH2_ADDRESSES"]:
            validator = get_validator(addr=address)
            logger.info(f"Added validator from ETH2_ADDRESSES - {validator}")
            validators.add(validator)

    if type(config.get("ETH1_ADDRESSES")) == list:
        for eth1_address in config["ETH1_ADDRESSES"]:
            for validator in get_validators_for_eth1_address(eth1_address):
                logger.info(
                    f"Added validator from ETH1_ADDRESSES [{eth1_address}] - {validator}"
                )
                validators.add(validator)

    # Remove validators with index set to 0 - pending validators with no processed eth2 deposit
    val_pending = lambda x: x.validator_index == 0
    for v in filter(val_pending, validators):
        logger.warning(f"Removed validator with 0 index (pending) - {v}")
    validators = list(filterfalse(val_pending, validators))

    return validators

def get_datapoints_for_slot(slot: int) -> List[DataPoint]:
    """Returns DataPoint objects for the specified slot."""
    validator_indexes = [idx for idx, addr in validators]

    if len(validator_indexes) == 0:
        return []

    slot_datetime = GENESIS_DATETIME + datetime.timedelta(seconds=slot * SLOT_TIME)

    logger.debug(f"Getting data for slot {slot}")

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
    now = UTC_TIMEZONE.localize(datetime.datetime.utcnow())
    start_date = datetime.datetime.fromisoformat(config["START_DATE"])
    start_date = UTC_TIMEZONE.localize(start_date)
    start_date = max(start_date, GENESIS_DATETIME)
    end_date = datetime.datetime.fromisoformat(config["END_DATE"]) + datetime.timedelta(
        days=1
    )
    end_date = UTC_TIMEZONE.localize(end_date)
    end_date = min(end_date, now)

    # Calculate the slot numbers at which we need to retrieve the balance
    slot_numbers = []
    # Calculate the initial balance at the start of START_DATE
    initial_slot_no = slot_no_for_datetime(start_date)
    slot_numbers.append(initial_slot_no)

    current_date = start_date
    while current_date < end_date:
        last_slot_datetime = current_date.replace(hour=23, minute=59, second=59)
        last_slot_datetime = min(last_slot_datetime, now)

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
                if resp.status_code != 200:
                    raise Exception(f"Non-200 status code received from CoinGecko - "
                                    f"{resp.status_code} - {resp.content.decode()}")
                cg_data = resp.json()
            except Exception as e:
                logger.error(
                    "Something went wrong while fetching price data from CoinGecko"
                )
                raise e
            eth_price[dp.datetime.strftime(format="%d-%m-%Y")] = cg_data["market_data"][
                "current_price"
            ][currency.lower()]

    for validator_index, eth2_address in tqdm(
        validators, desc="Writing rewards to file"
    ):
        validator_datapoints = [
            dp for dp in datapoints if dp.validator_index == validator_index
        ]

        if len(validator_datapoints) == 0:
            logger.info(
                f"[!] No datapoints for validator {validator_index} {eth2_address}"
            )
            continue

        with open(f"rewards_{validator_index}_{eth2_address}.csv", "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=config["CSV"]["DELIMITER"])
            if config["CSV"]["ADD_SEP_LINE"]:
                csvfile.write(f"sep={writer.dialect.delimiter}\n")

            writer.writerow(
                [
                    "Date",
                    "End-of-day balance [ETH]",
                    "Income for date [ETH]",
                    f"Price for date [{currency}/ETH]",
                    f"Income for date [{currency}]",
                ]
            )

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
    global validators
    validators = get_validators()
    datapoints = get_all_datapoints()
    write_rewards_to_file(datapoints=datapoints)


if __name__ == "__main__":
    main()
