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

aave_v2_pool_address = "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9".removeprefix("0x")
aave_v2_pool_abi = json.load(open("aave/abi/aave_v2_pool_abi.json"))

aave_v2_pool_decoder = ContractDecoder(w3, aave_v2_pool_abi)
Borrow_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["Borrow"]
    )
).decode()
Deposit_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["Deposit"]
    )
).decode()
FlashLoan_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["FlashLoan"]
    )
).decode()
LiquidationCall_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["LiquidationCall"]
    )
).decode()
Paused_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["Paused"]
    )
).decode()
RebalanceStableBorrowRate_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["RebalanceStableBorrowRate"]
    )
).decode()
Repay_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["Repay"]
    )
).decode()
ReserveDataUpdated_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["ReserveDataUpdated"]
    )
).decode()
ReserveUsedAsCollateralDisabled_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["ReserveUsedAsCollateralDisabled"]
    )
).decode()
ReserveUsedAsCollateralEnabled_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["ReserveUsedAsCollateralEnabled"]
    )
).decode()
Swap_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["Swap"]
    )
).decode()
Unpaused_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["Unpaused"]
    )
).decode()
Withdraw_topic = hexlify(
    eth_utils.event_abi_to_log_topic(
        aave_v2_pool_decoder.event_abi_dict["Withdraw"]
    )
).decode()

data = {
    "Borrow": [],
    "Deposit": [],
    "FlashLoan": [],
    "LiquidationCall": [],
    "Paused": [],
    "RebalanceStableBorrowRate": [],
    "Repay": [],
    "ReserveDataUpdated": [],
    "ReserveUsedAsCollateralDisabled": [],
    "ReserveUsedAsCollateralEnabled": [],
    "Swap": [],
    "Unpaused": [],
    "Withdraw": [],
}

limit = 100_000
offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{Borrow_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("Borrow", log=event)
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
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{Deposit_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("Deposit", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Deposit"].extend(docs)

    if len(events) < limit:
        print("Deposit", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{FlashLoan_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("FlashLoan", log=event)
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
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{LiquidationCall_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("LiquidationCall", log=event)
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
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{Paused_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("Paused", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"]
        log["transactionIndex"] = event["transactionIndex"]
        log["logIndex"] = event["logIndex"]
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Paused"].extend(docs)

    if len(events) < limit:
        print("Paused", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{RebalanceStableBorrowRate_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("RebalanceStableBorrowRate", log=event)
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
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{Repay_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("Repay", log=event)
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
    events = ky_eth.events(
        f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{ReserveDataUpdated_topic}') ",
        limit=limit,
        offset=offset,
    )
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("ReserveDataUpdated", log=event)
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
    events = ky_eth.events(f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{ReserveUsedAsCollateralDisabled_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("ReserveUsedAsCollateralDisabled", log=event)
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
    events = ky_eth.events(f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{ReserveUsedAsCollateralEnabled_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("ReserveUsedAsCollateralEnabled", log=event)
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
    events = ky_eth.events(f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{Swap_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("Swap", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"] 
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Swap"].extend(docs)

    if len(events) < limit:
        print("Swap", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{Unpaused_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("Unpaused", log=event)
        log["timestamp"] = event["blockTimestamp"]
        log["blockNumber"] = event["blockNumber"] 
        log["transactionIndex"] = event["transactionIndex"] 
        log["logIndex"] = event["logIndex"] 
        log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
        docs.append(log)

    data["Unpaused"].extend(docs)

    if len(events) < limit:
        print("Unpaused", len(docs))
        break
    else:
        offset += limit

offset = 0
while True:
    events = ky_eth.events(f"address = unhex('{aave_v2_pool_address}') and topics[1] = unhex('{Withdraw_topic}') ", limit=limit, offset=offset)
    docs = []
    for event in events:
        log = aave_v2_pool_decoder.decode_event_log("Withdraw", log=event)
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

pickle.dump(data, open("examples/aave_v2_pool_events.pickle", "wb"))
