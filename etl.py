import psycopg2
import pandas as pd
import os

DATA_DIR = "data/"
PROCESSED_DIR = "processed/"

conn = psycopg2.connect(
    dbname="cryptodb", user="postgres", password="password", host="localhost"
)

queries = {
    "high_value": "sql/01_high_value_failed_transactions.sql",
    "wallet_summary": "sql/02_wallet_behavior_summary.sql",
    "token_movement": "sql/03_token_movement_by_wallet.sql",
    "eth_token_flow": "sql/04_eth_token_flow_joins.sql",
    "internal_fund_flow": "sql/05_internal_fund_flow_chains.sql",
    "wallet_risk": "sql/06_wallet_risk_ranking.sql"
}

for name, path in queries.items():
    with open(path, 'r') as file:
        query = file.read()
    df = pd.read_sql_query(query, conn)
    df.to_csv(f"data/{name}.csv", index=False)
    print(f"{name}.csv exported.")



def load_csv(filename):
    return pd.read_csv(os.path.join(DATA_DIR, filename))

def transform_high_value(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["is_suspicious"] = df["tx_type"] != "Normal transaction"
    return df

def transform_eth_token_flow(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["value_eth_norm"] = df["value_eth"] / df["value_eth"].max()
    df["value_token_norm"] = df["value_token"] / df["value_token"].max()
    return df

def transform_token_movement(df):
    df["total_tokens_sent_norm"] = df.groupby("token_symbol")["total_tokens_sent"] \
                                     .transform(lambda x: x / x.max())
    return df

def transform_wallet_risk(df):
    df["risk_score"] = (
        df["failed_count"] * 2 +
        df["high_value_count"] * 3 +
        df["gas_flag_count"] * 1
    )
    return df

def transform_wallet_summary(df):
    df["normalized_total_sent"] = df["total_sent_eth"] / df["total_sent_eth"].max()
    return df

def transform_internal_fund_flows(df):
    df["is_large"] = df["value_eth"] > 1
    return df

def save(df, name):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_parquet(os.path.join(PROCESSED_DIR, f"{name}.parquet"))
    print(f"Saved: {name}.parquet")

def main():

    wallet_df = transform_wallet_summary(load_csv("wallet_summary.csv"))
    save(wallet_df, "wallet_summary")

    fund_flow_df = transform_internal_fund_flows(load_csv("internal_fund_flow.csv"))
    save(fund_flow_df, "internal_fund_flow")

    high_value_df = transform_high_value(load_csv("high_value.csv"))
    save(high_value_df, "high_value")

    eth_token_df = transform_eth_token_flow(load_csv("eth_token_flow.csv"))
    save(eth_token_df, "eth_token_flow")

    token_move_df = transform_token_movement(load_csv("token_movement.csv"))
    save(token_move_df, "token_movement")

    risk_df = transform_wallet_risk(load_csv("wallet_risk.csv"))
    save(risk_df, "wallet_risk")


if __name__ == "__main__":
    main()



