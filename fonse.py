path = "/data"
redis_host = "localhost"
redis_port = 6379

import redis
import pandas as pd
from datetime import datetime, timedelta
from zipfile import ZipFile
import io
import json
import logging as log
import traceback
import glob

log.basicConfig(level=log.NOTSET, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")

try:
    log.info("Script for FONSE started")
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    lastknownstocks = r.hgetall("lastKnownStockValue")

    for k in lastknownstocks:
        t = json.loads(lastknownstocks[k])
        lastknownstocks[k] = t

    one_day = timedelta(1)
    date_today = datetime.now() - one_day
    file_date_str = datetime.strftime(date_today, "%d%b%Y").upper()
    file_year_str = datetime.strftime(date_today, "%Y")
    file_month_str = datetime.strftime(date_today, "%b").upper()

    # url = f"{path}/fo{file_date_str}bhav.csv.zip"

    # Get file with wildcard support
    file_list = glob.glob(f"{path}/fo*")
    if file_list:
        url = file_list[0]
        log.info(f"Using file {url}")
    else:
        raise FileNotFoundError("File for FONSE not found")

    fonse = pd.read_csv(url, dtype=str, compression="zip")

    fonse_token_mapper = pd.read_csv(
        f"{path}/FONSEScripMaster.txt",
        dtype=str,
    )

    fonse_token_mapper.rename(
        columns={
            "Token": "TOKEN",
            "InstrumentName": "INSTRUMENT",
            "ExchangeCode": "SYMBOL",
            "ExpiryDate": "EXPIRY_DT",
            "StrikePrice": "STRIKE_PR",
            "OptionType": "OPTION_TYP",
        },
        errors="ignore",
        inplace=True,
    )

    fonse_token_mapper.loc[fonse_token_mapper["SYMBOL"] == "NIFTY 50", "SYMBOL"] = "NIFTY"
    fonse_token_mapper.loc[fonse_token_mapper["SYMBOL"] == "NIFTY BANK", "SYMBOL"] = "BANKNIFTY"
    fonse_token_mapper.loc[fonse_token_mapper["SYMBOL"] == "NIFTY FINANCIAL", "SYMBOL"] = "FINNIFTY"
    fonse_token_mapper.loc[fonse_token_mapper["SYMBOL"] == "NIFTY MIDCAP", "SYMBOL"] = "MIDCPNIFTY"

    fonse_token_mapper2 = fonse_token_mapper[
        ["INSTRUMENT", "SYMBOL", "EXPIRY_DT", "STRIKE_PR", "OPTION_TYP", "TOKEN"]
    ]

    fonse2 = pd.merge(
        left=fonse,
        right=fonse_token_mapper2,
        on=["INSTRUMENT", "SYMBOL", "EXPIRY_DT", "STRIKE_PR", "OPTION_TYP"],
        how="left",
    )

    missing_tokens = fonse2[fonse2["TOKEN"].isnull()]
    log.warn(f"Total {len(missing_tokens)} entries have missing tokens")
    log.warn(missing_tokens)

    fonse2 = fonse2[fonse2["TOKEN"].notnull()]

    fonse2["iTOKEN"] = "4.1!" + fonse2["TOKEN"]

    fonse2["CLOSE"] = pd.to_numeric(fonse2["CLOSE"])

    fonse_dict = fonse2[["iTOKEN", "CLOSE"]].set_index("iTOKEN").T.to_dict()

    fonse_dict_close = {key : fonse_dict[key]["CLOSE"] for key in fonse_dict}

    missed = 0

    for k in lastknownstocks:
        if k.startswith("4.1"):
            if k in fonse_dict:
                lastknownstocks[k][22] = fonse_dict[k]["CLOSE"]
                lastknownstocks[k][5] = round(
                    (
                        (lastknownstocks[k][2] - fonse_dict[k]["CLOSE"])
                        / fonse_dict[k]["CLOSE"]
                    )
                    * 100,
                    2,
                )
            else:
                missed += 1

    # log.info(f"Missed script {missed}")

    for k in lastknownstocks:
        t = json.dumps(lastknownstocks[k])
        lastknownstocks[k] = t

    r.hset("lastKnownStockValue", mapping=lastknownstocks)

    r.hset("close", mapping=fonse_dict_close)

    log.info("Script for FONSE succeeded.")
except Exception:
    log.error(traceback.format_exc())
