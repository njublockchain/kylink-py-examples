import os
import json
from kylink.eth.base import EthereumProvider
from web3 import Web3
import pickle
from kylink.evm import ContractDecoder
from binascii import hexlify
import dotenv
import eth_utils

dotenv.load_dotenv()

ky_eth = EthereumProvider(
    api_token=os.environ["KYLINK_API_TOKEN"],
    host="localhost",
    port=18123,
    interface="http",
)
w3 = Web3()

aave_v3_pool_address = "0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf".removeprefix("0x")
aave_v3_pool_abi = json.load(open("aave/abi/aave_v3_pool_abi.json"))

aave_v3_pool_decoder = ContractDecoder(w3, aave_v3_pool_abi)
BackUnbacked_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["BackUnbacked"]
    )
).decode()
Borrow_topic = hexlify(
    eth_utils.event_abi_to_log_topic(aave_v3_pool_decoder.event_abi_dict["Borrow"])
).decode()
FlashLoan_topic = hexlify(
    eth_utils.event_abi_to_log_topic(aave_v3_pool_decoder.event_abi_dict["FlashLoan"])
).decode()
IsolationModeTotalDebtUpdated_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["IsolationModeTotalDebtUpdated"]
    )
).decode()
LiquidationCall_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["LiquidationCall"]
    )
).decode()
MintUnbacked_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["MintUnbacked"]
    )
).decode()
MintedToTreasury_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["MintedToTreasury"]
    )
).decode()
RebalanceStableBorrowRate_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["RebalanceStableBorrowRate"]
    )
).decode()
Repay_topic = hexlify(
    eth_utils.event_abi_to_log_topic(aave_v3_pool_decoder.event_abi_dict["Repay"])
).decode()
ReserveDataUpdated_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["ReserveDataUpdated"]
    )
).decode()
ReserveUsedAsCollateralDisabled_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["ReserveUsedAsCollateralDisabled"]
    )
).decode()
ReserveUsedAsCollateralEnabled_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["ReserveUsedAsCollateralEnabled"]
    )
).decode()
Supply_topic = hexlify(
    eth_utils.event_abi_to_log_topic(aave_v3_pool_decoder.event_abi_dict["Supply"])
).decode()
SwapBorrowRateMode_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["SwapBorrowRateMode"]
    )
).decode()
UserEModeSet_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v3_pool_decoder.event_abi_dict["UserEModeSet"]
    )
).decode()
Withdraw_topic = hexlify(
    eth_utils.event_abi_to_log_topic(aave_v3_pool_decoder.event_abi_dict["Withdraw"])
).decode()

limit = 10_000
data = {
    "BackUnbacked": [],
    "Borrow": [],
    "FlashLoan": [],
    "IsolationModeTotalDebtUpdated": [],
    "LiquidationCall": [],
    "MintUnbacked": [],
    "MintedToTreasury": [],
    "RebalanceStableBorrowRate": [],
    "Repay": [],
    "ReserveDataUpdated": [],
    "ReserveUsedAsCollateralDisabled": [],
    "ReserveUsedAsCollateralEnabled": [],
    "Supply": [],
    "SwapBorrowRateMode": [],
    "UserEModeSet": [],
    "Withdraw": [],
}

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{BackUnbacked_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("BackUnbacked", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["BackUnbacked"].extend(docs)

    if len(events) < limit:
        print("BackUnbacked", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{Borrow_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("Borrow", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Borrow"].extend(docs)

    if len(events) < limit:
        print("Borrow", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{FlashLoan_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("FlashLoan", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["FlashLoan"].extend(docs)

    if len(events) < limit:
        print("FlashLoan", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{IsolationModeTotalDebtUpdated_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log(
            "IsolationModeTotalDebtUpdated", log=event
        )
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["IsolationModeTotalDebtUpdated"].extend(docs)

    if len(events) < limit:
        print("IsolationModeTotalDebtUpdated", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{LiquidationCall_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("LiquidationCall", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["LiquidationCall"].extend(docs)

    if len(events) < limit:
        print("LiquidationCall", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{MintUnbacked_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("MintUnbacked", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["MintUnbacked"].extend(docs)

    if len(events) < limit:
        print("MintUnbacked", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{MintedToTreasury_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("MintedToTreasury", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["MintedToTreasury"].extend(docs)

    if len(events) < limit:
        print("MintedToTreasury", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{RebalanceStableBorrowRate_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log(
            "RebalanceStableBorrowRate", log=event
        )
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["RebalanceStableBorrowRate"].extend(docs)

    if len(events) < limit:
        print("RebalanceStableBorrowRate", len(docs))
        break
    else:
        offset += limit
    
offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{Repay_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("Repay", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Repay"].extend(docs)

    if len(events) < limit:
        print("Repay", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{ReserveDataUpdated_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("ReserveDataUpdated", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["ReserveDataUpdated"].extend(docs)

    if len(events) < limit:
        print("ReserveDataUpdated", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{ReserveUsedAsCollateralDisabled_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("ReserveUsedAsCollateralDisabled", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["ReserveUsedAsCollateralDisabled"].extend(docs)

    if len(events) < limit:
        print("ReserveUsedAsCollateralDisabled", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{ReserveUsedAsCollateralEnabled_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("ReserveUsedAsCollateralEnabled", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["ReserveUsedAsCollateralEnabled"].extend(docs)

    if len(events) < limit:
        print("ReserveUsedAsCollateralEnabled", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{Supply_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("Supply", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Supply"].extend(docs)

    if len(events) < limit:
        print("Supply", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{SwapBorrowRateMode_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("SwapBorrowRateMode", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["SwapBorrowRateMode"].extend(docs)

    if len(events) < limit:
        print("SwapBorrowRateMode", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{UserEModeSet_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("UserEModeSet", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["UserEModeSet"].extend(docs)

    if len(events) < limit:
        print("UserEModeSet", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v3_pool_address}') and topics[1] = unhex('{Withdraw_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v3_pool_decoder.decode_event_log("Withdraw", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Withdraw"].extend(docs)

    if len(events) < limit:
        print("Withdraw", len(docs))
        break
    else:
        offset += limit

pickle.dump(data, open("examples/aave_v3_pool_events.pickle", "wb"))
