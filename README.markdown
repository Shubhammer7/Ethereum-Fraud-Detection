# 🕵️‍♂️ Ethereum Fraud Detection via Smart Contract Analytics

## 📌 Overview

This project applies data science / machine-learning techniques to detect suspicious activity on the Ethereum blockchain. It analyzes wallet behavior, smart contract interactions, and token transfers to identify patterns such as high-risk wallets, multi-hop fund flows, and token dumps.

## 📚 Methodology

- **Data Source**: Etherscan API, focused on `uniswap_v3_router` and related wallets between Jan 1–15, 2024.
- **Data Collection**: `web.py` fetches transactions, token transfers, and internal fund flows using the Etherscan API.
- **Database**: PostgreSQL with a normalized schema storing transactions, token transfers, and address labels.
- **SQL Analysis**: Custom `.sql` files (`sql/*.sql`) rank wallets, detect multi-hop flows, and identify high-risk interactions.
- **ETL**: `etl.py` extracts, transforms, and stores data as `.parquet` files for efficient analysis.
- **Analysis**: `fraud_analysis.ipynb` provides statistical exploration, visualizations, and anomaly detection using IsolationForest.
- **OSINT**: `osint.py` fetches Etherscan labels for suspicious wallets to support de-anonymization.

## 📊 Notable Features

- **Risk Scoring System**: Heuristic-based scoring for wallets based on failed transactions, high-value transfers, and gas usage, visualized in the [Top 10 Risky Wallets by Heuristic Score](#) plot.
- **Multi-Hop Fund Tracing**: Detects fund obfuscation through internal smart contract calls, shown in the [Distribution of Trace Depth](#) histogram.
- **Statistical Validation**: Pearson correlation and KS tests validate behavioral patterns (e.g., p < 0.05 for ETH sent vs. failed transactions).
- **Normalized Token vs ETH Behavior Plots**: Visualizes combined ETH-token movements.
- **OSINT Integration**: Labels suspicious wallets based on interactions with known DeFi contracts.

## 📈 Visual Insights
- **Distribution of Total ETH Sent by Wallets**: Highlights the skewed distribution of wallet activity, with most wallets sending low ETH and a few high-activity wallets (e.g., 1666 ETH), as shown in the ![dist_total_eth](https://github.com/Shubhammer7/Ethereum-Fraud-Detection/blob/main/graphs/dist_total_eth.png).
- **Top 10 Risky Wallets by Heuristic Score**: Displays the highest-risk wallets, aiding prioritization for investigation, with scores up to ~300.

## 🚀 Setup Instructions

1. **Clone Repository**:
   ```bash
   git clone <https://github.com/Shubhammer7/Ethereum-Fraud-Detection>
   cd ethereum-fraud-detection
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   Requirements: `pandas`, `psycopg2-binary`, `requests`, `scikit-learn`, `matplotlib`, `seaborn`, `numpy`, `scipy`, `python-dateutil`.

3. **Set Up PostgreSQL**:
   - Install PostgreSQL and create a database named `cryptodb`.
   - Update `DB_PARAMS` in `web.py` and `etl.py` with your credentials (e.g., user, password).

4. **Run Data Collection** (if needed):
   ```bash
   python web.py
   ```
   Select contract(s) and time period(s) to fetch data.

5. **Run ETL Pipeline** (if needed):
   ```bash
   python etl.py
   ```

6. **Run OSINT Analysis**:
   ```bash
   python osint.py
   ```
   **Note**: Replace the `API_KEY` variable in `web.py, osint.py` with your own Etherscan API key for OSINT functionality.

7. **Analyze Data**:
   Open `fraud_analysis.ipynb` in Jupyter Notebook:
   ```bash
   jupyter notebook fraud_analysis.ipynb
   ```

## ✅ Outcome

The project demonstrates a scalable, SQL-first, and Python-driven fraud analysis pipeline, identifying:
- High-risk wallets with repeated failed or high-value transactions.
- Multi-hop ETH flows suggesting obfuscation.
- Potential token dumps via combined ETH-token transactions.
- Statistically significant behavioral differences (p < 0.05).

## 🔮 Future Extensions

- **OSINT Expansion**: Integrate social media data (e.g., Twitter API) to de-anonymize wallet owners further.
- **Broader DeFi Scope**: Extend to protocols like Aave, Curve, or Balancer to detect rug pulls, wash trading, or flash loan attacks.
- **Automation**: Schedule `web.py` and `osint.py` for continuous monitoring of suspicious addresses.
