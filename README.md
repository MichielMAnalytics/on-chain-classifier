# On-Chain Alert System

This project is an on-chain monitoring and alert system designed to track blockchain addresses associated with specific entities, identify new related addresses, monitor their transactions, and generate alerts based on predefined signals.

## Core Components

### 1. Address Identification (`address_identification.py`)

This module is responsible for discovering and tracking blockchain addresses related to a set of initial entities.

**Initial Setup & Discovery:**
*   The process starts with a list of seed entities defined in `variables.py`, which include initial contract addresses or known entity identifiers on platforms like Arkham.
*   For entities without a single base address, the system can perform a root identification step (`root_identification.py`) to find a primary address based on transaction patterns and filters.
*   It uses an iterative, stack-based approach:
    1.  An initial address (entity root) is added to a processing stack.
    2.  Transactions for the address on top of the stack are fetched via an API (e.g., Arkham).
    3.  These transactions are processed by `process_addresses` to identify new potentially related addresses.

**Criteria for Following New Addresses:**
*   **`tx_to_follow` Conditions**: `process_addresses` applies several conditions to decide if an interacting address should be further investigated:
    *   **First IN transaction**: The first ETH/WETH transaction received by an address.
    *   **Percentage of Total Value**: Outgoing transactions that move a significant percentage (`pct_total_value_threshold`) of the original value received by the root address (after the original funding transaction).
    *   **Interaction Frequency**: Addresses that have a high number of IN or OUT interactions (`no_of_in_interactions_threshold`, `no_of_out_interactions_threshold`).
    *   **USD Value Threshold**: Outgoing transactions exceeding a certain USD value (`USD_tx_threshold`).
*   **Exchange Deposit Address Identification**:
    *   The system identifies addresses that send funds to known exchange deposit addresses (excluding certain entities like "Copper" or "DepositAndPlaceOrder" if specified).
    *   It then looks at the addresses that previously sent funds *to these identified exchange deposit addresses*, effectively tracing funds one hop back from an exchange deposit. This helps identify potential source wallets.
*   **Known Entity Transactions**: Transactions involving addresses directly labeled by Arkham as belonging to the entity are also included.

**Data Persistence:**
*   All fetched transactions (`df_all_transactions`) and the list of identified addresses (`identified_addresses`) are stored in memory during processing.
*   The system can save its state (transactions and addresses) to pickle files (`mnt/data/all_transactions_df.pkl`, `mnt/data/all_identified_addresses.pkl`) using the `save_state` function (in `supporting_functions.py`).
*   Intermediate state can be saved during the `address_identification` phase after each address is processed.
*   **`START_FROM_STATE`**: If the environment variable `START_FROM_STATE` is set to `True`, the application will load the previously saved state using `load_state`, bypassing the initial intensive address identification process for already processed entities.

### 2. Transaction Monitoring & Signal Generation (`monitoring_and_alert_generation.py` & `signal_functions.py`)

Once a set of addresses is identified (either from a fresh run or loaded from state), this component monitors them for new activity and evaluates predefined signals.

**Fetching New Transactions:**
*   The `monitor_transactions` function periodically queries an API for new transactions for each identified address, using the last seen transaction timestamp for that address to avoid reprocessing old data.

**Signal Definitions (`signal_functions.py`):**
The system checks for various on-chain signals. If a signal's conditions are met for a new transaction, an alert is typically generated.
*   **S1 - Fresh Wallet (`S1_fresh_wallet`)**:
    *   Checks if an interacting address has a total USD balance <= 0.
    *   Checks if the wallet holds only Ethereum (with a zero balance).
    *   Checks if there has been no (or minimal, e.g., <=3) transaction activity in the recent past (e.g., last 90 days).
    *   *Condition for alert*: `(total_usd_bool OR only_ethereum) AND no_activity_last_period` and `historicalUSD` of the triggering transaction is above `usd_threshold`.
*   **S2 - Interaction with New Protocol (`S2_interaction_new_protocol`)**:
    *   Identifies if an address interacts with an Arkham-labeled entity (protocol, excluding CEXes) it hasn't interacted with before (based on historical transactions in `df_all_transactions`).
    *   *Condition for alert*: Interaction is with a new protocol AND `historicalUSD` of the transaction > 0.
*   **S3 - Interaction with New Crypto Token (`S3_interaction_new_crypto`)**:
    *   Detects if an address transacts with a token symbol it hasn't used before (excluding very recent transactions).
    *   *Condition for alert*: Interaction involves a new token symbol AND `historicalUSD` of the transaction is above `usd_threshold`.
*   **S4 - Interaction with New Exchange (`S4_interaction_new_exchange`)**:
    *   Checks if an address sends funds to a Centralized Exchange (CEX) it hasn't interacted with previously.
    *   *Condition for alert*: Interaction is with a new CEX AND `historicalUSD` of the transaction is above `usd_threshold`.
*   **S5a - Daily Cumulative Volume Exceeded (`S5a_daily_volume_alert_cum`)**:
    *   For outgoing transactions from a root address to a specific interacting address, it calculates the total USD volume for the current day.
    *   *Condition for alert*: Today's cumulative volume to that specific counterparty exceeds a relative threshold (`S5_relative_threshold`) of the entity's "Assets Under Management" ballpark figure (`AOM_ballpark`) AND `historicalUSD` of the current transaction is above `usd_threshold`. This alert is sent once per day per address pair.
*   **S5b - Daily Absolute Volume Exceeded (`S5b_daily_volume_alert_abs`)**:
    *   For a single outgoing transaction.
    *   *Condition for alert*: The `historicalUSD` value of the transaction exceeds an absolute threshold (entity's `AOM_ballpark`) AND `historicalUSD` of the current transaction is above `usd_threshold`. This alert is sent once per day per address pair.
*   **S6a - Daily Frequency Change (`S6a_d_freq_change`)**:
    *   Monitors the daily transaction frequency between a root address and an interacting address (for transactions > $100).
    *   *Condition for alert*: The change in today's transaction frequency compared to the historical mean frequency is greater than a specified number of standard deviations (`num_std_dev_S6a_d`) AND today's frequency is >= 10. This alert is sent once per day per address pair.
*   **S6b - Weekly Frequency Change (`S6b_w_freq_change`)**:
    *   Similar to S6a, but calculates frequency on a weekly basis.
    *   *Condition for alert*: The change in the current week's transaction frequency compared to the historical mean weekly frequency is greater than `num_std_dev_S6b_w` standard deviations AND current week's frequency is >= 10. This alert is sent once per day per address pair.
*   **S7 - Protocol Activity Change (`S7_protocol_activity`)**: (Active if `ACTIVE_PROTOCOL_SIGNAL` env var is True)
    *   Tracks changes in an address's portfolio balances across different DeFi protocols (requires DeBank API key).
    *   Compares current protocol balances (obtained after a new transaction) with balances from the most recent prior transaction for that address.
    *   *Condition for alert*: A change of `perc_threshold` (e.g., 20%) or more in balance for any protocol.
*   **S8 - LP Token Traded (`S8_LP_token_traded`)**:
    *   Detects if a transaction involves a token whose symbol or name contains "LP" (case-insensitive).
    *   *Condition for alert*: An LP token is part of the transaction.

**Alerting (`messaging_functions.py`):**
*   If any of the above signals are triggered, the `construct_alert_message` function formats a message.
*   `send_telegram_message` sends this message to a specified Telegram chat via the Telegram Bot API.
*   The system also supports Telegram polling (`telegram_polling`) for commands, such as removing addresses from monitoring.

## Key Files

*   **`main.py`**: The main script that initializes the system, orchestrates the address identification (if not loading from state), and starts the continuous monitoring loop.
*   **`address_identification.py`**: Contains all logic related to discovering and identifying new blockchain addresses based on initial entities and transaction patterns.
*   **`monitoring_and_alert_generation.py`**: Handles the continuous polling for new transactions for identified addresses and triggers the evaluation of signals for each new transaction.
*   **`signal_functions.py`**: Defines the specific logic for each alert signal (S1 through S8).
*   **`supporting_functions.py`**: Provides various helper functions, including API interactions, data processing, state saving (`save_state`, `append_state`), state loading (`load_state`), and logging setup.
*   **`variables.py`**: Stores global variables, thresholds used in identification and signaling (e.g., `USD_tx_threshold`, `no_of_in_interactions_threshold`), and the initial list of `entities` to track.
*   **`settings.py`**: Manages configuration values, primarily by fetching them from environment variables (e.g., API keys, API URLs, Telegram bot details).
*   **`messaging_functions.py`**: Contains functions for sending Telegram messages and handling incoming Telegram commands.
*   **`root_identification.py`**: Includes logic to determine a primary "root" address for an entity based on its transaction history if a direct root address isn't provided.

## Setup and Running

**Dependencies:**
*   Python 3.x
*   Pandas
*   Requests
*   Numpy
*   (It's good practice to have a `requirements.txt` file listing all dependencies.)

**Environment Variables:**
The application relies on several environment variables for configuration. These should be set in your environment or a `.env` file if using a library like `python-dotenv`. Critical variables include:
*   `API_KEY`: Arkham API Key.
*   `API_KEY_DEBANK`: DeBank API Key (for S7 signal).
*   `BOT_TOKEN`: Telegram Bot Token.
*   `CHAT_ID`: Telegram Chat ID to send alerts to.
*   `START_FROM_STATE`: Set to `True` or `1` to load data from previously saved state files (`mnt/data/*.pkl`). Defaults to `False`.
*   `ACTIVE_PROTOCOL_SIGNAL`: Set to `True` to enable the S7 signal (protocol balance checking).
*   `RUNNING_IN_ACI`: Set to `True` if running in Azure Container Instances (affects file paths for logs/data).

**Running the Application:**
1.  Ensure all dependencies are installed.
2.  Set up the required environment variables.
3.  Execute the main script:
    ```bash
    python main.py
    ```
    To run from a saved state:
    ```bash
    export START_FROM_STATE=True
    python main.py
    ```
    Or, if using a `.env` file, ensure `START_FROM_STATE=True` is set there.

## How it Works - Flow

1.  **Initialization (`main.py`):**
    *   Sets up logging.
    *   Initializes a multiprocessing manager and a queue for handling commands (e.g., address removal via Telegram).
    *   Starts a separate process for Telegram polling (`telegram_polling`).
    *   Reads environment variables (e.g., `START_FROM_STATE`).

2.  **Address Identification / State Loading:**
    *   **If `START_FROM_STATE` is `False` (or not set):**
        *   Iterates through each entity in `variables.py`.
        *   Performs root identification if necessary (`extract_entity_root`).
        *   Calls `address_identification` to discover and collect all related addresses and their transactions. This involves the stack-based processing and exchange deposit logic.
        *   Known entity transactions are added using `add_known_entity_transactions`.
        *   The collected `all_transactions_df` and `all_identified_addresses` are saved using `append_state` (to append to or create state files) and `save_addresses` (for a separate CSV, potentially for review).
    *   **If `START_FROM_STATE` is `True`:**
        *   Calls `load_state` to load `all_transactions_df` and `all_identified_addresses` from previously saved pickle files in `mnt/data/`.

3.  **Monitoring Initialization (`initialize_monitoring`):**
    *   Prepares data structures for monitoring, such as `df_signals` and `last_seen_tx_timestamps` (initialized to the current time or loaded transaction times).
    *   May fetch initial protocol balances for known addresses if S7 is active.

4.  **Main Monitoring Loop (`while True` in `main.py`):**
    *   **Daily Report**: At a specific time (e.g., "17:00"), `send_daily_report` might be called.
    *   **Monitor Transactions (`monitor_transactions`):**
        *   Iterates through each `address` in `all_identified_addresses`.
        *   Fetches new transactions since `last_seen_tx_timestamps[address]` using the API.
        *   If new transactions are found:
            *   Helper columns are added (e.g., `tx_direction`, `interacting_address`).
            *   Protocol balances might be updated for the S7 signal.
            *   `df_all_transactions` is updated.
            *   `last_seen_tx_timestamps` for the address is updated.
            *   Each new transaction (`row`) is then evaluated against signals S1-S8.
            *   If a signal function returns `True` (or meets its specific alert criteria):
                *   A new row is added to `df_signals`.
                *   An alert message is constructed and sent via Telegram.
                *   For certain signals (like S1, S5b), `process_signalised_address` might be called to add the interacting address to `identified_addresses` for further tracking if it's an unknown external address.
                *   Newly discovered addresses from signals are processed by `get_extra_transactions` to fetch their history, which is added to `df_all_transactions`.
        *   The `df_signals` (containing triggered alerts) is appended to `mnt/data/df_signals.csv`.
    *   **Handle Removal Queue**: Checks if the `removal_queue` (populated by Telegram commands) has any addresses to stop monitoring. If so, `stop_monitoring` updates `all_transactions_df` and `all_identified_addresses`.
    *   **Save State**: If the number of transactions or identified addresses has changed during the loop iteration, `save_state` is called to persist the latest `all_transactions_df` and `all_identified_addresses` to the pickle files. `save_addresses` is also called.

This cycle of fetching, processing, signaling, and saving continues indefinitely. 