import pandas as pd
import requests
import time
import os

API_KEY = 'API_KEY'
BASE_URL = 'https://api.etherscan.io/api'
RATE_LIMIT_DELAY = 0.25
PROCESSED_DIR = "processed/"


def fetch_etherscan_labels(address):
    try:
        url = f"{BASE_URL}?module=account&action=txlist&address={address}&apikey={API_KEY}"
        response = requests.get(url, timeout=10)
        data = response.json()
        if response.status_code == 200 and data.get("status") == "1":
            txs = data["result"]
            known_contracts = {
                "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D": "Uniswap V2",
                "0xE592427A0AEce92De3Edee1F18E0157C05861564": "Uniswap V3",
                "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F": "Sushiswap",
                "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9": "Aave",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7": "USDT",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": "USDC",
                "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": "WETH"
            }
            for tx in txs:
                to_addr = tx.get("to", "").lower()
                if to_addr in known_contracts:
                    return f"{known_contracts[to_addr]} Interaction", "DeFi"
            return "Unknown Wallet", "Individual"
        return "Unknown Wallet", "Individual"
    except Exception as e:
        print(f"Error fetching label for {address}: {e}")
        return "Unknown Wallet", "Individual"
    finally:
        time.sleep(RATE_LIMIT_DELAY)


def process_osint(wallet_risk_file):
    try:
        df = pd.read_parquet(wallet_risk_file)
        suspicious_wallets = df[df["risk_score"] > 3]["sender"].unique()
        osint_data = []
        for addr in suspicious_wallets:
            label, category = fetch_etherscan_labels(addr)
            osint_data.append({"sender": addr, "label": label, "category": category})
            print(f"Processed OSINT for {addr}: {label}, {category}")
        osint_df = pd.DataFrame(osint_data)
        osint_df.to_parquet(os.path.join(PROCESSED_DIR, "osint_labels.parquet"))
        print("Saved: osint_labels.parquet")
        return osint_df
    except Exception as e:
        print(f"Error processing OSINT: {e}")
        return pd.DataFrame()


def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    wallet_risk_file = os.path.join(PROCESSED_DIR, "wallet_risk.parquet")
    osint_df = process_osint(wallet_risk_file)
    print(osint_df.head())


if __name__ == "__main__":
    main()