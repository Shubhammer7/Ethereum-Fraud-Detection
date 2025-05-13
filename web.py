import requests
import psycopg2
import datetime
import time
import sys
import json
import os
from dateutil.relativedelta import relativedelta

API_KEY = 'API_KEY'
BASE_URL = 'https://api.etherscan.io/api'

#This is my personal limit left after testing, please modify according to your liking
MAX_API_CALLS_PER_DAY = 98000  #These were my remaining calls
RATE_LIMIT_DELAY = 0.25  #This is set to 4 requests per second, but etherscan actually allows 5/sec
STATE_FILE = "eth_scan_state.json"

# Tracking api_calls in order to avoid hitting the limit
api_calls_made = 0

# CHANGE THIS to your liking
DB_PARAMS = {
    "host": "localhost",
    "database": "cryptodb",
    "user": "postgres",
    "password": "password"
}

# LIST OF SMART CONTRACT ADDRESSES TO SELECT FROM
# I aligned my project to focus more on swaps and some normal transactions
ADDRESSES = {
    # Decentralized Exchanges (well-known)
    "uniswap_v2_router": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
    "uniswap_v3_router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    "sushiswap_router": "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F",

    # Known flash loan providers
    "aave_lending_pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",

    # Some known fraudulent/hacked addresses (examples)
    "nomad_bridge_hack": "0x56D8B635A5C25B4d3C982fF6a7D7b9570F0f9F4D",

    # Uniswap core contracts (they are notorious to always be in the middle of fraud and hacks)
    "uniswap_v2_factory": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
    "uniswap_v3_factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",

    # USDT, USDC, WETH (common tokens in fraud scenarios)
    "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
}

# These are a few periods with significant fraudulent activity
TIME_PERIODS = [
    # Periods with major hacks or exploits
    {"name": "Nomad Bridge Hack", "start_date": "2022-08-01", "end_date": "2022-08-03"},
    {"name": "Wormhole Exploit", "start_date": "2022-02-02", "end_date": "2022-02-03"},

    # First days of each quarter in 2023 - good for sampling activity over time
    {"name": "Q1 2023 Sample", "start_date": "2023-01-01", "end_date": "2023-01-03"},
    {"name": "Q2 2023 Sample", "start_date": "2023-04-01", "end_date": "2023-04-03"},
    {"name": "Q3 2023 Sample", "start_date": "2023-07-01", "end_date": "2023-07-03"},
    {"name": "Q4 2023 Sample", "start_date": "2023-10-01", "end_date": "2023-10-03"},

    # Recent activity (2024)
    {"name": "Recent Activity", "start_date": "2024-01-01", "end_date": "2024-01-15"}
]

# ERC-20 Token signatures and methods to watch for in input data
TOKEN_SIGNATURES = {
    "transfer": "0xa9059cbb",
    "transferFrom": "0x23b872dd",
    "approve": "0x095ea7b3",
    "swap": "0x022c0d9f"
}

#SQL related stuff
def ensure_tables_exist():
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS internal_transactions (
                tx_hash TEXT PRIMARY KEY,
                block_number INTEGER,
                timestamp TIMESTAMP WITHOUT TIME ZONE,
                sender TEXT,
                receiver TEXT,
                value_eth NUMERIC,
                gas BIGINT,
                gas_used BIGINT,
                tx_type TEXT,
                is_error BOOLEAN
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS token_transfers (
                tx_hash TEXT,
                block_number INTEGER,
                timestamp TIMESTAMP WITHOUT TIME ZONE,
                token_address TEXT,
                from_address TEXT,
                to_address TEXT,
                value_token NUMERIC,
                token_name TEXT,
                token_symbol TEXT,
                token_decimals INTEGER,
                PRIMARY KEY (tx_hash, token_address, from_address, to_address)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS eth_internal_txs (
                tx_hash TEXT,
                block_number INTEGER,
                timestamp TIMESTAMP WITHOUT TIME ZONE,
                from_address TEXT,
                to_address TEXT,
                value_eth NUMERIC,
                trace_id TEXT,
                error TEXT,
                call_type TEXT,
                PRIMARY KEY (tx_hash, trace_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS address_labels (
                address TEXT PRIMARY KEY,
                label TEXT,
                category TEXT,
                known_entity BOOLEAN,
                first_seen TIMESTAMP WITHOUT TIME ZONE,
                last_seen TIMESTAMP WITHOUT TIME ZONE
            )
        """)

        conn.commit()
        print("Database tables verified/created successfully")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()


def save_state(state_dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(state_dict, f)
    print(f"State saved to {STATE_FILE}")


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return None


def track_api_call():
    global api_calls_made
    api_calls_made += 1

    if api_calls_made >= MAX_API_CALLS_PER_DAY:
        print(f"WARNING: Maximum API calls reached ({api_calls_made}). Exiting.")
        sys.exit(1)
    elif api_calls_made >= MAX_API_CALLS_PER_DAY * 0.9:
        print(f"WARNING: Approaching API call limit ({api_calls_made}/{MAX_API_CALLS_PER_DAY})")

    # Prints status every 10 calls
    if api_calls_made % 10 == 0:
        print(f"API calls made: {api_calls_made}/{MAX_API_CALLS_PER_DAY}")


def timestamp_to_block(timestamp):
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": int(timestamp),
        "closest": "before",
        "apikey": API_KEY
    }

    try:
        track_api_call()
        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()

        if response.status_code == 200 and data.get("status") == "1":
            return int(data["result"])
        else:
            print(f"Error converting timestamp to block: {data.get('message')}")
            return None
    except Exception as e:
        print(f"Error in timestamp_to_block: {e}")
        return None


def get_transactions_by_time_period(address, period, action="txlist"):
    start_date = datetime.datetime.fromisoformat(period["start_date"])
    end_date = datetime.datetime.fromisoformat(period["end_date"])

    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    start_block = timestamp_to_block(start_timestamp)
    end_block = timestamp_to_block(end_timestamp)

    if not start_block or not end_block:
        print(f"Could not determine block numbers for period {period['name']}")
        return []

    print(f"Fetching {action} for {period['name']} (Blocks {start_block} to {end_block})")
    return get_transactions(address, start_block, end_block, action)


def get_transactions(address, start_block, end_block, action="txlist"):
    all_txs = []
    block_step = 10000
    current_block = start_block

    while current_block <= end_block:
        next_block = min(current_block + block_step - 1, end_block)
        params = {
            "module": "account",
            "action": action,
            "address": address,
            "startblock": current_block,
            "endblock": next_block,
            "sort": "asc",
            "apikey": API_KEY
        }

        max_retries = 3
        success = False

        for retry in range(max_retries):
            try:
                track_api_call()
                response = requests.get(BASE_URL, params=params, timeout=20)
                data = response.json()

                print(f"Querying blocks {current_block:,} to {next_block:,}")

                if response.status_code == 200 and data.get("status") == "1":
                    txs = data["result"]
                    if txs:
                        all_txs.extend(txs)
                        print(f"Found {len(txs)} transactions")
                        save_state({
                            "address": address,
                            "last_processed_block": next_block + 1,
                            "action": action,
                            "api_calls_made": api_calls_made,
                            "txs_found": len(all_txs)
                        })
                    success = True
                    break
                elif response.status_code == 200 and data.get("status") == "0":
                    if "rate limit" in data.get("message", "").lower():
                        print(f"Rate limit exceeded. Waiting for 5 seconds...")
                        time.sleep(5)
                        continue
                    else:
                        print(f"API returned message: {data.get('message')}")
                        success = True
                        break
                else:
                    print(f"Error: {response.status_code}, {data.get('message', 'Unknown error')}")
                    time.sleep(1 * (retry + 1))
            except Exception as e:
                print(f"Exception during API call: {e}")
                time.sleep(2 * (retry + 1))

        if not success:
            print(f"Failed after {max_retries} retries for blocks {current_block} to {next_block}")

        current_block = next_block + 1
        time.sleep(RATE_LIMIT_DELAY)

    return all_txs


def get_internal_transactions(txhash):
    params = {
        "module": "account",
        "action": "txlistinternal",
        "txhash": txhash,
        "apikey": API_KEY
    }

    try:
        track_api_call()
        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()

        if response.status_code == 200 and data.get("status") == "1":
            return data["result"]
        else:
            print(f"Error getting internal transactions: {data.get('message')}")
            return []
    except Exception as e:
        print(f"Error in get_internal_transactions: {e}")
        return []


def get_token_transfers(address, start_block, end_block):
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "sort": "asc",
        "apikey": API_KEY
    }

    try:
        track_api_call()
        response = requests.get(BASE_URL, params=params, timeout=20)
        data = response.json()

        if response.status_code == 200 and data.get("status") == "1":
            return data["result"]
        elif response.status_code == 200 and data.get("status") == "0":
            if "No transactions found" in data.get("message", ""):
                print(f"No token transfers found for {address}")
                return []
            else:
                print(f"API returned message: {data.get('message')}")
                return []
        else:
            print(f"Error: {response.status_code}, {data.get('message', 'Unknown error')}")
            return []
    except Exception as e:
        print(f"Error in get_token_transfers: {e}")
        return []


def get_internal_transactions_by_address(address, start_block, end_block):
    params = {
        "module": "account",
        "action": "txlistinternal",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "sort": "asc",
        "apikey": API_KEY
    }

    try:
        track_api_call()
        response = requests.get(BASE_URL, params=params, timeout=20)
        data = response.json()

        if response.status_code == 200 and data.get("status") == "1":
            return data["result"]
        elif response.status_code == 200 and data.get("status") == "0":
            if "No transactions found" in data.get("message", ""):
                print(f"No internal transactions found for {address}")
                return []
            else:
                print(f"API returned message: {data.get('message')}")
                return []
        else:
            print(f"Error: {response.status_code}, {data.get('message', 'Unknown error')}")
            return []
    except Exception as e:
        print(f"Error in get_internal_transactions_by_address: {e}")
        return []


def get_wallet_addresses_from_transactions(transactions, limit=10):
    addresses = {}

    for tx in transactions:
        sender = tx.get('from', '').lower()
        receiver = tx.get('to', '').lower()

        # Skip empty addresses and contracts
        if sender and sender not in addresses:
            # Check if this might be a wallet (not a contract)
            if len(tx.get('input', '')) <= 10:  # wallets usually send simple transactions
                addresses[sender] = addresses.get(sender, 0) + 1

        if receiver and receiver not in addresses:
            addresses[receiver] = addresses.get(receiver, 0) + 1

    sorted_addresses = sorted(addresses.items(), key=lambda x: x[1], reverse=True)
    return [addr for addr, count in sorted_addresses[:limit]]


def analyze_and_extract_suspicious(transactions):
    suspicious = []

    for tx in transactions:
        value_eth = float(tx.get('value', '0')) / 1e18

        if value_eth > 50:
            tx['flag_reason'] = "High value transaction"
            suspicious.append(tx)
            continue

        if tx.get('isError') == '1':
            tx['flag_reason'] = "Failed transaction"
            suspicious.append(tx)
            continue

        if int(tx.get('gasUsed', 0)) > 1000000:
            tx['flag_reason'] = "High gas consumption"
            suspicious.append(tx)
            continue

        if tx.get('to') == '' or tx.get('to') is None:
            tx['flag_reason'] = "Contract creation"
            suspicious.append(tx)
            continue

        input_data = tx.get('input', '').lower()
        suspicious_methods = ['flashloan', 'flash', 'swap', 'arbitrage']

        if any(method in input_data for method in suspicious_methods):
            tx['flag_reason'] = "Suspicious method call"
            suspicious.append(tx)
            continue

        input_data = tx.get('input', '').lower()
        for method, signature in TOKEN_SIGNATURES.items():
            if input_data.startswith(signature):
                tx['flag_reason'] = f"Token {method} operation"
                suspicious.append(tx)
                break

    print(f"Extracted {len(suspicious)} suspicious transactions from {len(transactions)} total")
    return suspicious


def insert_transactions(transactions, table_name="internal_transactions"):
    if not transactions:
        print("No transactions to insert")
        return

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        inserted = 0
        for tx in transactions:
            try:
                flag_reason = tx.get('flag_reason', '')

                cursor.execute(f"""
                    INSERT INTO {table_name} (
                        tx_hash, block_number, timestamp,
                        sender, receiver, value_eth,
                        gas, gas_used, tx_type, is_error
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_hash) DO NOTHING;
                """, (
                    tx.get('hash', ''),
                    int(tx.get('blockNumber', 0)),
                    datetime.datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                    tx.get('from', ''),
                    tx.get('to', ''),
                    float(tx.get('value', '0')) / 1e18,
                    int(tx.get('gas', 0)),
                    int(tx.get('gasUsed', 0)),
                    flag_reason or tx.get('type', ''),
                    tx.get('isError', '0') == '1'
                ))
                inserted += 1

                if inserted % 100 == 0:
                    conn.commit()
                    print(f"Committed {inserted} transactions so far")
            except Exception as e:
                print(f"Error inserting tx {tx.get('hash', 'unknown')}: {e}")

        conn.commit()
        print(f"Successfully inserted {inserted} transactions into {table_name}")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def insert_token_transfers(transfers):
    if not transfers:
        print("No token transfers to insert")
        return

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        inserted = 0
        for transfer in transfers:
            try:
                cursor.execute("""
                    INSERT INTO token_transfers (
                        tx_hash, block_number, timestamp,
                        token_address, from_address, to_address,
                        value_token, token_name, token_symbol, token_decimals
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_hash, token_address, from_address, to_address) DO NOTHING;
                """, (
                    transfer.get('hash', ''),
                    int(transfer.get('blockNumber', 0)),
                    datetime.datetime.fromtimestamp(int(transfer.get('timeStamp', 0))),
                    transfer.get('contractAddress', ''),
                    transfer.get('from', ''),
                    transfer.get('to', ''),
                    float(transfer.get('value', '0')) / (10 ** int(transfer.get('tokenDecimal', 18))),
                    transfer.get('tokenName', ''),
                    transfer.get('tokenSymbol', ''),
                    int(transfer.get('tokenDecimal', 18))
                ))
                inserted += 1

                if inserted % 100 == 0:
                    conn.commit()
                    print(f"Committed {inserted} token transfers so far")
            except Exception as e:
                print(f"Error inserting token transfer {transfer.get('hash', 'unknown')}: {e}")

        conn.commit()
        print(f"Successfully inserted {inserted} token transfers")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def insert_internal_transactions(internal_txs):
    if not internal_txs:
        print("No internal transactions to insert")
        return

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        inserted = 0
        for tx in internal_txs:
            try:
                cursor.execute("""
                    INSERT INTO eth_internal_txs (
                        tx_hash, block_number, timestamp,
                        from_address, to_address, value_eth,
                        trace_id, error, call_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_hash, trace_id) DO NOTHING;
                """, (
                    tx.get('hash', ''),
                    int(tx.get('blockNumber', 0)),
                    datetime.datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                    tx.get('from', ''),
                    tx.get('to', ''),
                    float(tx.get('value', '0')) / 1e18,
                    tx.get('traceId', ''),
                    tx.get('isError', ''),
                    tx.get('type', '')
                ))
                inserted += 1

                # Commit in batches
                if inserted % 100 == 0:
                    conn.commit()
                    print(f"Committed {inserted} internal transactions so far")
            except Exception as e:
                print(f"Error inserting internal tx {tx.get('hash', 'unknown')}: {e}")

        conn.commit()
        print(f"Successfully inserted {inserted} internal transactions")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def insert_address_label(address, label, category):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        now = datetime.datetime.now()

        cursor.execute("""
            INSERT INTO address_labels (
                address, label, category, known_entity, first_seen, last_seen
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE SET
                label = EXCLUDED.label,
                category = EXCLUDED.category,
                last_seen = EXCLUDED.last_seen;
        """, (
            address.lower(),
            label,
            category,
            True,
            now,
            now
        ))

        conn.commit()
    except Exception as e:
        print(f"Error inserting address label: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def process_regular_transactions(address, period, action="txlist"):
    transactions = get_transactions_by_time_period(address, period, action)

    if not transactions:
        print(f"No transactions found for {address} in period {period['name']}")
        return []

    insert_transactions(transactions)

    return transactions

def process_token_transfers(address, period):
    start_date = datetime.datetime.fromisoformat(period["start_date"])
    end_date = datetime.datetime.fromisoformat(period["end_date"])

    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    start_block = timestamp_to_block(start_timestamp)
    end_block = timestamp_to_block(end_timestamp)

    if not start_block or not end_block:
        print(f"Could not determine block numbers for period {period['name']}")
        return []

    print(f"Fetching token transfers for {period['name']} (Blocks {start_block} to {end_block})")
    token_transfers = get_token_transfers(address, start_block, end_block)

    if not token_transfers:
        print(f"No token transfers found for {address} in period {period['name']}")
        return []

    insert_token_transfers(token_transfers)

    return token_transfers


def process_internal_transactions(address, period):
    start_date = datetime.datetime.fromisoformat(period["start_date"])
    end_date = datetime.datetime.fromisoformat(period["end_date"])

    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())

    start_block = timestamp_to_block(start_timestamp)
    end_block = timestamp_to_block(end_timestamp)

    if not start_block or not end_block:
        print(f"Could not determine block numbers for period {period['name']}")
        return []

    print(f"Fetching internal transactions for {period['name']} (Blocks {start_block} to {end_block})")
    internal_txs = get_internal_transactions_by_address(address, start_block, end_block)

    if not internal_txs:
        print(f"No internal transactions found for {address} in period {period['name']}")
        return []

    insert_internal_transactions(internal_txs)

    return internal_txs

def process_wallet_addresses(regular_txs, period):
    wallet_addresses = get_wallet_addresses_from_transactions(regular_txs)

    if not wallet_addresses:
        print("No wallet addresses identified for tracking")
        return

    print(f"Identified {len(wallet_addresses)} potential wallet addresses to track")

    for i, address in enumerate(wallet_addresses):
        insert_address_label(address, f"Wallet {i + 1}", "Individual Wallet")

        print(f"Processing wallet address: {address}")
        wallet_txs = process_regular_transactions(address, period)

        process_token_transfers(address, period)

        process_internal_transactions(address, period)

def clear_database():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        tables = ["internal_transactions", "token_transfers", "eth_internal_txs", "address_labels"]

        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            print(f"Cleared table: {table}")

        conn.commit()
        print("All database tables cleared successfully")
    except Exception as e:
        print(f"Error clearing database: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


def main():
    global api_calls_made

    print("\n=== Ethereum Fraud Detection Data Collection ===")
    print(f"API Usage Limit: {MAX_API_CALLS_PER_DAY} calls remaining")

    # Ensure tables exist
    ensure_tables_exist()

    # Check if we should resume from previous state
    saved_state = load_state()
    if saved_state:
        print(f"Found saved state: {saved_state}")
        resume = input("Do you want to resume from the saved state? (y/n): ").lower()
        if resume == 'y':
            api_calls_made = saved_state.get("api_calls_made", 0)
            print(f"Resuming with {api_calls_made} API calls already made")

    # Ask if user wants to clear existing data
    clear_data = input("\nDo you want to clear existing data in the database? (y/n): ").lower()
    if clear_data == 'y':
        clear_database()
        print("Database cleared. Starting fresh data collection.")
    else:
        print("Keeping existing data. New data will be added without duplicates.")

    print("\nThis enhanced script addresses the gaps identified in your fraud detection ETL pipeline:")
    print("1. Adding token transfer data (action=tokentx)")
    print("2. Including normal wallets to trace individual bad actors")
    print("3. Using internalTx API to track fund flows across multiple hops")
    print("\nChoose a data collection strategy:")
    print("1. Focused collection (specific time periods and contracts)")
    print("2. Comprehensive collection (all data types for maximum coverage)")
    print("3. Token transfers only (focus on ERC-20 token movements)")
    print("4. Internal transactions only (focus on fund flows)")
    print("5. Wallet tracing (identify and trace individual wallets)")

    choice = input("Enter choice (1-5): ")

    if choice == "1":
        print("\nRunning focused collection...")

        print("\nSelect contract(s) to analyze:")

        for i, (name, addr) in enumerate(ADDRESSES.items(), 1):
            print(f"{i}. {name} ({addr})")

        contract_choices = input("Enter contract numbers (comma-separated): ")
        selected_contracts = [list(ADDRESSES.items())[int(c.strip()) - 1] for c in contract_choices.split(",")]

        print("\nSelect time period(s) to analyze:")
        for i, period in enumerate(TIME_PERIODS, 1):
            print(f"{i}. {period['name']} ({period['start_date']} to {period['end_date']})")

        period_choices = input("Enter period numbers (comma-separated): ")
        selected_periods = [TIME_PERIODS[int(p.strip()) - 1] for p in period_choices.split(",")]

        for contract_name, contract_address in selected_contracts:
            for period in selected_periods:
                print(f"\nProcessing {contract_name} for {period['name']}:")

                print("\n> Processing regular transactions...")
                regular_txs = process_regular_transactions(contract_address, period)

                print("\n> Processing token transfers...")
                process_token_transfers(contract_address, period)

                print("\n> Processing internal transactions...")
                process_internal_transactions(contract_address, period)

                print("\n> Processing wallet addresses...")
                process_wallet_addresses(regular_txs, period)

                if api_calls_made > MAX_API_CALLS_PER_DAY * 0.8:
                    print("⚠️ Approaching API limit. Saving progress and exiting.")
                    sys.exit(0)

    elif choice == "2":
        print("\nRunning comprehensive collection. This will use a significant portion of your API calls.")
        confirm = input("Are you sure you want to proceed? (y/n): ").lower()

        if confirm != 'y':
            print("Operation canceled.")
            return

        print("\nSelect time period(s) to analyze:")
        for i, period in enumerate(TIME_PERIODS, 1):
            print(f"{i}. {period['name']} ({period['start_date']} to {period['end_date']})")

        period_choices = input("Enter period numbers (comma-separated): ")
        selected_periods = [TIME_PERIODS[int(p.strip()) - 1] for p in period_choices.split(",")]

        for period in selected_periods:
            print(f"\nProcessing period: {period['name']}")

            for contract_name, contract_address in ADDRESSES.items():
                print(f"\nProcessing contract: {contract_name} ({contract_address})")

                print("> Processing regular transactions...")
                txs = process_regular_transactions(contract_address, period)

                print("> Processing token transfers...")
                process_token_transfers(contract_address, period)

                print("> Processing internal transactions...")
                process_internal_transactions(contract_address, period)

                suspicious_txs = analyze_and_extract_suspicious(txs)

                for stx in suspicious_txs[:10]:
                    tx_hash = stx.get('hash')
                    print(f"Investigating suspicious transaction: {tx_hash}")

                    internal_txs = get_internal_transactions(tx_hash)
                    if internal_txs:
                        insert_internal_transactions(internal_txs)

                if api_calls_made > MAX_API_CALLS_PER_DAY * 0.8:
                    print("Approaching API limit. Saving progress and exiting.")
                    sys.exit(0)

    elif choice == "3":
        print("\nFocusing on token transfers...")

        print("\nSelect contract(s) to analyze:")

        for i, (name, addr) in enumerate(ADDRESSES.items(), 1):
            print(f"{i}. {name} ({addr})")

        contract_choices = input("Enter contract numbers (comma-separated): ")
        selected_contracts = [list(ADDRESSES.items())[int(c.strip()) - 1] for c in contract_choices.split(",")]

        print("\nSelect time period(s) to analyze:")
        for i, period in enumerate(TIME_PERIODS, 1):
            print(f"{i}. {period['name']} ({period['start_date']} to {period['end_date']})")

        period_choices = input("Enter period numbers (comma-separated): ")
        selected_periods = [TIME_PERIODS[int(p.strip()) - 1] for p in period_choices.split(",")]

        for contract_name, contract_address in selected_contracts:
            for period in selected_periods:
                print(f"\nProcessing token transfers for {contract_name} during {period['name']}:")
                process_token_transfers(contract_address, period)

    elif choice == "4":
        print("\nFocusing on internal transactions...")

        print("\nSelect contract(s) to analyze:")

        for i, (name, addr) in enumerate(ADDRESSES.items(), 1):
            print(f"{i}. {name} ({addr})")

        contract_choices = input("Enter contract numbers (comma-separated): ")
        selected_contracts = [list(ADDRESSES.items())[int(c.strip()) - 1] for c in contract_choices.split(",")]

        print("\nSelect time period(s) to analyze:")
        for i, period in enumerate(TIME_PERIODS, 1):
            print(f"{i}. {period['name']} ({period['start_date']} to {period['end_date']})")

        period_choices = input("Enter period numbers (comma-separated): ")
        selected_periods = [TIME_PERIODS[int(p.strip()) - 1] for p in period_choices.split(",")]

        for contract_name, contract_address in selected_contracts:
            for period in selected_periods:
                print(f"\nProcessing internal transactions for {contract_name} during {period['name']}:")
                process_internal_transactions(contract_address, period)

    elif choice == "5":
        print("\nFocusing on wallet tracing...")

        print("\nFirst, we need to get some transactions to identify wallets.")
        print("Select a contract to start with:")

        for i, (name, addr) in enumerate(ADDRESSES.items(), 1):
            print(f"{i}. {name} ({addr})")

        contract_choice = int(input("Enter contract number: ")) - 1
        contract_name, contract_address = list(ADDRESSES.items())[contract_choice]

        print("\nSelect a time period to analyze:")
        for i, period in enumerate(TIME_PERIODS, 1):
            print(f"{i}. {period['name']} ({period['start_date']} to {period['end_date']})")

        period_choice = int(input("Enter period number: ")) - 1
        selected_period = TIME_PERIODS[period_choice]

        print(f"\nProcessing {contract_name} for {selected_period['name']} to identify wallets:")

        regular_txs = process_regular_transactions(contract_address, selected_period)

        print("\nProcessing identified wallet addresses...")
        process_wallet_addresses(regular_txs, selected_period)

    print(f"\nScript completed with {api_calls_made} API calls.")

if __name__ == "__main__":
    main()