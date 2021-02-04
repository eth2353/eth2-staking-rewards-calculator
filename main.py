import csv
import datetime
import logging
import math
import sys
from collections import namedtuple
from multiprocessing.pool import ThreadPool
import codecs
from base64 import b64encode

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pycoingecko import CoinGeckoAPI
from tqdm import tqdm
from yaml import safe_load

DataPoint = namedtuple("DataPoint", ["datetime", "balance"])
SLOT_TIME = 12
SLOTS_IN_EPOCH = 32
GENESIS_DATETIME = datetime.datetime.fromtimestamp(1606824023)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout
)

logger = logging.getLogger()
cg = CoinGeckoAPI()

with open("config.yml") as f:
    config = safe_load(f)


def get_datapoint(input: list) -> DataPoint:
    """Returns a DataPoint for the specified slot and address."""
    slot, address = input
    host = config["BEACON_NODE"]["HOST"]
    port = config["BEACON_NODE"]["PORT"]
    prysm_api = config["BEACON_NODE"]["PRYSM_API"]

    # Retry a few times with exponential backoff
    s = requests.Session()

    retries = Retry(total=5,
                    backoff_factor=1,
                    status_forcelist=[500],
                    raise_on_status=False)

    s.mount('http://', HTTPAdapter(max_retries=retries))

    logger.debug(f"Getting data for slot {slot}, public key {address}")
    if prysm_api:
        encoded_pubkey = b64encode(codecs.decode(address[2:], "hex"))
        epoch_for_slot = math.floor(slot / SLOTS_IN_EPOCH)

        resp = s.get(
            f"http://{host}:{port}/eth/v1alpha1/validators/balances",
            params={"epoch": epoch_for_slot, "publicKeys": encoded_pubkey},
        )
    else:
        resp = s.get(
            f"http://{host}:{port}/eth/v1/beacon/states/{slot}/validator_balances",
            params={"id": address},
        )

    if resp.status_code != 200:
        raise Exception(
            f"Failed to retrieve data from beacon node - request {resp.request.url} -> "
            f"[{resp.status_code}] [{resp.content.decode()}]"
        )

    if prysm_api:
        data = resp.json()["balances"][0]
    else:
        data = resp.json()["data"][0]

    slot_datetime = GENESIS_DATETIME + datetime.timedelta(seconds=slot * SLOT_TIME)
    balance = int(data["balance"]) / 1000000000

    return DataPoint(datetime=slot_datetime, balance=balance)


def get_datapoints() -> dict:
    """
    Retrieves rewards data for all addresses from the beacon node.

    Returns:
        A dictionary, in which the validator public keys are the
            dictionary keys and the value is a list of DataPoints.
    """
    start_date = datetime.datetime.fromisoformat(config["START_DATE"])
    end_date = datetime.datetime.fromisoformat(config["END_DATE"]) + datetime.timedelta(
        days=1
    )

    # Calculate the slot numbers at which we need to retrieve the balance
    slot_numbers = []
    current_date = start_date
    while current_date < end_date:
        last_slot_datetime = current_date.replace(hour=23, minute=59, second=59)
        last_slot_no = math.floor(
            (last_slot_datetime - GENESIS_DATETIME).total_seconds() / 12
        )
        slot_numbers.append(last_slot_no)

        # Move on to the next day
        current_date = current_date + datetime.timedelta(days=1)

    logger.debug(
        f"Calculating rewards for slots {min(slot_numbers)} - {max(slot_numbers)}"
    )

    datapoint_dict = {}
    for address in tqdm(config["ETH2_ADDRESSES"], desc="Addresses"):
        addr_datapoints = []
        with ThreadPool(config["BEACON_NODE"]["CONCURRENT_REQUESTS"]) as p:
            inputs = [(slot, address) for slot in slot_numbers]
            for dp in tqdm(
                p.imap(get_datapoint, inputs),
                desc=f"Retrieving beacon chain data for address " f"{address[:10]}...",
                total=len(inputs),
            ):
                addr_datapoints.append(dp)
            datapoint_dict[address] = addr_datapoints

    return datapoint_dict


def write_rewards_to_file(datapoint_dict: dict):
    """
    Processes the datapoints, retrieves corresponding price data
    and writes it to a file.
    """
    currency = config["CURRENCY"]

    for address, datapoints in tqdm(
        datapoint_dict.items(), desc=f"Getting price data and writing rewards to file"
    ):
        with open(f"rewards_{address}.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "Date",
                    "End-of-day balance [ETH]",
                    "Income for date [ETH]",
                    f"Price for date [{currency}/ETH]",
                    f"Income for date [{currency}]",
                ]
            )
            prev_balance = config["INITIAL_BALANCE"]
            total_income_eth = 0
            total_income_curr = 0
            for dp in datapoints:
                cg_data = cg.get_coin_history_by_id(
                    "ethereum", date=dp.datetime.strftime(format="%d-%m-%Y")
                )
                eth_price = cg_data["market_data"]["current_price"][currency.lower()]

                income_for_date_eth = dp.balance - prev_balance
                total_income_eth += income_for_date_eth

                income_for_date_curr = eth_price * income_for_date_eth
                total_income_curr += income_for_date_curr

                writer.writerow(
                    [
                        dp.datetime.strftime(format="%Y-%m-%d"),
                        dp.balance,
                        income_for_date_eth,
                        eth_price,
                        income_for_date_curr,
                    ]
                )
                prev_balance = dp.balance
            writer.writerow(["Total:", "", total_income_eth, "", total_income_curr])
    logger.info("All done!")


def main():
    dp_dict = get_datapoints()
    write_rewards_to_file(datapoint_dict=dp_dict)


if __name__ == "__main__":
    main()
