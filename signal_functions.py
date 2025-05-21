import requests
import pandas as pd
import requests
import time
import numpy as np
from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil.tz import tzutc

from settings import get_config
import requests
from settings import url_transfers, url_coingecko, get_address_history_url, no_of_days_ago_s1 ,num_std_dev_S6a_d, num_std_dev_S6b_w, S5_relative_threshold

config_values = get_config()
api_key = config_values['api_key']
import logging
logger = logging.getLogger(__name__)
import os

######################################

def S1_fresh_wallet(address, limit=1000, offset=0):
    logger.info("processing S1_fresh_wallet..")
    '''
    This function checks the status of a wallet address by making a GET request to the Arkham Intelligence API.
    
    Args:
        address (str): The wallet address to check.
        
    Returns:
        total_usd_bool (bool): True if the total USD balance for the address is 0 or less, False otherwise.
        total_usd (int): The total USD balance for the address.
        only_ethereum (bool): True if the only asset in the wallet is Ethereum and the balance is 0, False otherwise.
        no_activity_last_period (bool): False if there was any transaction activity in the wallet in the past 90 days, True otherwise.
        
    Raises:
        Exception: If the API request fails, an exception is raised with the failed status code.
    '''

    # Make sure necessary variables like api_key are globally available or defined elsewhere
    headers = {"API-Key": api_key}
    
    try:
        # Send a GET request to the API for history
        response_history = requests.get(get_address_history_url(address), headers=headers)
        response_history.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

    except requests.exceptions.HTTPError as http_err:
        logger.warning(f"S1_fresh_wallet: HTTP error occurred for address {address} when fetching history: {http_err} - Status Code: {response_history.status_code}")
        return False, None, False, False # Default/neutral value
    except requests.exceptions.RequestException as req_err:
        logger.warning(f"S1_fresh_wallet: Request error occurred for address {address} when fetching history: {req_err}")
        return False, None, False, False # Default/neutral value

    data = response_history.json()

    # Process history data
    total_usd = 0
    only_ethereum = True
    current_date_min_no_of_days_ago = datetime.now(tzutc()) - timedelta(days=no_of_days_ago_s1)
    
    for chain, history in data.items():
        history = sorted(history, key=lambda x: parse(x["time"]), reverse=True)
        chain_usd = 0
        
        if history:
            chain_usd = history[0]["usd"]
            total_usd += chain_usd
            total_usd = int(total_usd)

        if chain.lower() != 'ethereum' or chain_usd != 0:
            only_ethereum = False
            
    total_usd_bool = total_usd <= 0

    # Prepare the URL and parameters for the transfers endpoint
    params = {
        'base': address,
        'limit': limit,
        'offset': offset
    }
    
    try:
        # Send a GET request to the API for transfers
        response_transfers = requests.get(url_transfers, params=params, headers=headers)
        response_transfers.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)

    except requests.exceptions.HTTPError as http_err:
        logger.warning(f"S1_fresh_wallet: HTTP error occurred for address {address} when fetching transfers: {http_err} - Status Code: {response_transfers.status_code}")
        return False, None, False, False # Default/neutral value
    except requests.exceptions.RequestException as req_err:
        logger.warning(f"S1_fresh_wallet: Request error occurred for address {address} when fetching transfers: {req_err}")
        return False, None, False, False # Default/neutral value

    transfers = response_transfers.json()

     # Process transfers data to determine activity in the last 90 days
    transfers_list = transfers['transfers']

    if len(transfers_list) <= 3:
        # If there's only three transactions, we consider it as "no activity in the last period"
        no_activity_last_period = True
    else:
        no_activity_last_period = all(parse(transfer["blockTimestamp"]) < current_date_min_no_of_days_ago for transfer in transfers_list[3:])


    return total_usd_bool, total_usd, only_ethereum, no_activity_last_period

def S2_interaction_new_protocol(df, tx, recent_tx_threshold=2):

    logger.info("processing S2_interaction_new_protocol..")

    """
    Processes the given transaction to determine interactions based on the new protocol.

    This function checks if the `interacting_address` from the transaction has had
    previous interactions with any entities present in the transaction. For the purpose
    of this check, only certain entity-related fields are considered, and any missing
    fields are logged for informational purposes.

    Parameters:
    - df (pd.DataFrame): The dataframe containing historical transactions.
    - tx (dict): The transaction to be processed. Expected to contain an 'interacting_address'
                 and may have one or more entity-related fields.

    Returns:
    - tuple: A tuple containing three elements:
        1. bool: Indicates whether any valid entity-related fields were found in the transaction.
        2. list or None: List of entities found in the transaction or None if no valid entities were found.
        3. bool: Indicates whether the `interacting_address` from the transaction has had
                 any previous interactions with the entities in the transaction.

    Exceptions:
    - If a KeyError occurs, it's assumed a required field is missing from the transaction.
      A warning is logged, and the function returns (False, None, False).
    - For any other unexpected exception, an error is logged, and the function returns (False, None, False).

    Notes:
    - The function uses the logger to log various events such as missing fields and encountered exceptions.
    """

    try:  
        address = tx['interacting_address']
        current_tx_id = tx['id'] 
   
        # Check for historicalUSD value
        historical_usd = tx.get('historicalUSD')
        if historical_usd is None or not isinstance(historical_usd, (int, float)) or historical_usd <= 0:
            logger.info("S2_interaction_new_protocol: historicalUSD is missing, not a number, or not greater than 0. Skipping signal.")
            return False, None, False

        # Extract entity info from the current transaction
        entity_fields = ['toAddress.arkhamEntity.name', 'toAddress.arkhamEntity.type']

        # Check for missing fields in tx
        missing_fields = [field for field in entity_fields if field not in tx]

        # Log the missing fields for informational purposes
        if missing_fields:
            logger.warning(f"Fields {', '.join(missing_fields)} not found in the transaction.")

        # Extract values from available fields, ignoring the missing ones
        # Here we're also checking if the entity type is 'cex' and excluding it
        entities_in_tx = [
            tx[field] for field in entity_fields 
            if field not in missing_fields 
            and pd.notna(tx[field]) 
            and not (field == 'toAddress.arkhamEntity.type' and tx[field] == 'cex')
        ]


        # If no entity info is present in the transaction, return False, None, False
        if not entities_in_tx:
            return False, None, False

        # Check if any of the entities in the transaction have interacted with the address before & Exclude the current transaction from previous_txs by comparing a unique identifier
        previous_txs = df[(df['id'] != current_tx_id) & ((df['fromAddress.address'] == address) | (df['toAddress.address'] == address))].copy()
        previous_txs['blockTimestamp'] = pd.to_datetime(previous_txs['blockTimestamp'])
        previous_txs = previous_txs.sort_values('blockTimestamp', ascending=False).iloc[recent_tx_threshold:]  # Skip the most recent 2 transactions
        
        entity_interaction = any(previous_txs[field].isin(entities_in_tx).any() for field in entity_fields if field not in missing_fields)
        

        return True, entities_in_tx, entity_interaction

    except KeyError as e:
        logger.warning(f"Field {e} not found.")
        return False, None, False

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        return False, None, False


def S3_interaction_new_crypto(df, tx, recent_tx_threshold=1):
    logger.info("processing S3_interaction_new_crypto..")
    """
    This function checks whether the token involved in a transaction already exists in the transaction history
    for the specific address the transaction is associated with.

    :param df: DataFrame, all transactions until now
    :param tx: Series, new incoming transaction
    
    :return: tuple containing two elements - a boolean indicating whether the token is new and the token symbol
    """

    # Extract the token symbol and the address from the new transaction
    token_symbol = tx['tokenSymbol']
    address = tx['root_address']
    current_tx_id = tx['id'] 

    # Get all transactions for this address, excluding the current one and the most recent ones
    addr_txs = df[(df['root_address'] == address) | (df['interacting_address'] == address)]
    addr_txs = addr_txs[addr_txs['id'] != current_tx_id]  # exclude the current transaction

    # If there are fewer transactions than the recent_tx_threshold, it means they are all recent
    if len(addr_txs) <= recent_tx_threshold:
        return (True, token_symbol)

    # Exclude the most recent transactions based on the threshold
    addr_txs = addr_txs.iloc[:-recent_tx_threshold] if recent_tx_threshold > 0 else addr_txs

    # Check if the token symbol is in the list of token symbols for the transactions of this address
    if token_symbol not in addr_txs['tokenSymbol'].values:
        # If the token symbol is not in the list, return the tuple (True, token_symbol)
        return (True, token_symbol)
    else:
        # If the token symbol is in the list, return the tuple (False, None)
        return (False, None)





def S4_interaction_new_exchange(df, tx):
    logger.info("processing S4_interaction_new_exchange..")
    """
    This function checks whether the arkhamEntity.id involved in a transaction
    already exists in the transaction history for the specific address the transaction is associated with,
    and if the arkhamEntity.type is 'cex'.

    :param df_all_transactions: DataFrame, all transactions until now
    :param tx: Series, a single transaction
    
    :return: tuple containing three elements - a boolean indicating whether the arkhamEntity.id is new, and the arkhamEntity.id
    """
    # Try to extract the arkhamEntity.id and arkhamEntity.type, set to None if not present
    try:
        arkham_name = tx['toAddress.arkhamEntity.name']
        arkham_type = tx['toAddress.arkhamEntity.type']
        current_tx_id = tx['id']
    except KeyError:
        arkham_name = None
        arkham_type = None
        current_tx_id = None

    # Extract the address from the transaction
    address = tx['root_address']  # you might want to change this based on new logic

    # Get all transactions for this address excluding the current one
    addr_txs = df[(df['root_address'] == address) & (df['id'] != current_tx_id) & (df['tx_direction'] == 'out')]  # consider only outgoing transactions

    # Check if the arkhamEntity.name is in the list of arkhamEntity.names for the transactions of this address
    # We also check that the arkhamEntity.type is 'cex'
    # Make sure the arkhamEntity.id and arkhamEntity.type are not nan
    is_new_arkham_name = pd.notna(arkham_name) and pd.notna(arkham_type) and arkham_type == 'cex' and (arkham_name not in addr_txs['toAddress.arkhamEntity.name'].values if 'toAddress.arkhamEntity.name' in addr_txs.columns else [])

    # If the arkhamEntity.id is new and of type 'cex', return the tuple
    if is_new_arkham_name:
        return (True, arkham_name)
    else:
        return (False, None)


def create_exchange_rate_usd_dict(token_ids, delay=1):
    # API endpoint
    url = url_coingecko

    # Convert all token ids to lowercase and join them with commas
    ids_string = ','.join([id_.lower() for id_ in token_ids if id_ is not None])

    # Request the exchange rates
    params = {
        'ids': ids_string,
        'vs_currencies': 'usd'
    }

    exchange_rate_usd_dict = {}
    for id_ in token_ids:
        if id_ is not None:
            time.sleep(delay)  # add delay
            response = requests.get(url, params=params)
            
            # Check if request was successful
            if response.status_code != 200:
                logger.info(f"Request for {id_} failed with status code {response.status_code}")
                exchange_rate_usd_dict[id_] = np.nan
                continue
            
            data = response.json()
            id_lower = id_.lower()
            if id_lower in data:
                exchange_rate_usd_dict[id_] = data[id_lower]['usd']
            else:
                logger.info(f"Failed to get the exchange rate for {id_}. Response: {data}")
                exchange_rate_usd_dict[id_] = np.nan

    return exchange_rate_usd_dict

def calculate_daily_volume(df, exchange_rate_usd_dict):
    """
    Calculates the daily volume of transactions in USD for each address.

    Parameters:
    df (pandas.DataFrame): A DataFrame containing transaction data. Expected columns are 
                        'blockTimestamp', 'root_address', 'unitValue', and 'tokenId'.
    exchange_rate_usd_dict (dict): A dictionary mapping tokenId to its corresponding exchange rate in USD.

    Returns:
    pandas.Series: A Series with index 'blockTimestamp' containing the calculated
                daily volumes of transactions in USD.
    """
    
    df = df.copy()  # create a copy to prevent modifying original dataframe
    df = df.dropna(subset=['unitValue'])  # drop rows where 'unitValue' is None
    df['blockTimestamp'] = pd.to_datetime(df['blockTimestamp'])  # ensure 'blockTimestamp' is datetime
    df = df.set_index('blockTimestamp')  # set timestamp as index for easy resampling

    # calculate the volume in USD
    df['unitValue_usd'] = df.apply(lambda row: float(row['unitValue']) * exchange_rate_usd_dict.get(row['tokenId'], 1.0), axis=1)
    
    # calculate daily volume
    daily_volume = df['unitValue_usd'].resample('D').sum()

    return daily_volume

def S5a_daily_volume_alert_cum(df, tx, exchange_rate_usd_dict, entity):
    logger.info("processing S5a_daily_volume_alert_cum..")
    """
    Function to identify if the daily trading volume to a given address exceeds a prespecified relative threshold.

    Parameters:
    -----------
    df : pandas.DataFrame
        Dataframe containing all transaction data

    tx : pandas.Series
        A new transaction that needs to be appended to the dataframe for analysis.

    exchange_rate_usd_dict : dict
        Dictionary of exchange rates in USD.

    entity : dict
        The entity details including AOM_ballpark.

    Returns:
    --------
    alert : bool
        True if the trading volume for the current day exceeds the relative threshold, False otherwise.
    today_volume: float
         the total trading volume of today.
    threshold: float
        The threshold value that needs to be exceeded.

    Notes:
    ------
    The function works by first extracting all transactions for a given address. It then calculates the daily 
    trading volume and checks if it exceeds the relative threshold.
    """
    
    if tx['tx_direction'] != 'OUT':
        return False, 0, 0

   # Extract the address from the transaction
    root_address = tx['root_address']
    interacting_address = tx['interacting_address']  
    #tx_direction = tx['tx_direction']  # Get the direction of the transaction (IN or OUT)
    
    # Get all transactions from the root_address to the interacting_address including the new transaction
    filtered_rows = df[(df['root_address'] == root_address) & 
                       (df['tx_direction'] == 'OUT') &  # Use the transaction direction to filter rows
                       (df['interacting_address'] == interacting_address)]

    df_address_transactions = pd.concat([filtered_rows,tx]).drop_duplicates(subset=["transactionHash"])

    # Get daily volume
    daily_volume = calculate_daily_volume(df_address_transactions, exchange_rate_usd_dict)

    # check today's volume
    today_volume = daily_volume[-1] if len(daily_volume) >= 1 else 0

    # calculate the threshold based on the AOM_ballpark and S5_relative_threshold
    threshold = entity['AOM_ballpark'] * S5_relative_threshold

    # check if the daily volume exceeds the threshold
    alert = today_volume > threshold

    return alert, today_volume, threshold


def S5b_daily_volume_alert_abs(tx, entity):
    logger.info("processing S5b_daily_volume_alert_abs..")
    """
    Function to identify if the trading volume of a single 'OUT' transaction exceeds a prespecified absolute threshold.

    Parameters:
    -----------
    tx : pandas.Series
        A new transaction that needs to be analyzed.

    entity : dict
        The entity details including AOM_ballpark_abs.

    Returns:
    --------
    alert : bool
        True if the trading volume for the current transaction exceeds the absolute threshold, False otherwise.
    today_volume: float
         the trading volume of the transaction in USD.
    threshold: float
        The threshold value that needs to be exceeded.

    Notes:
    ------
    The function directly compares the historicalUSD value of a single 'OUT' transaction against a prespecified
    absolute threshold.
    """
    
    # If transaction direction is not 'OUT', return False for alert and 0 for today_volume and threshold
    if tx['tx_direction'] != 'OUT':
        return False, 0, 0
    
    try:
        # Try to get the volume of the single transaction in USD from historicalUSD column
        today_volume = tx['historicalUSD']
    except KeyError:
        logger.error(f"historicalUSD field not present in transaction: {tx}")
        # Return False for alert and 0 for today_volume and threshold if historicalUSD is not present
        return False, 0, 0
    
    # calculate the threshold based on the AOM_ballpark_abs
    threshold = entity['AOM_ballpark']
    
    # check if the transaction volume exceeds the threshold
    alert = today_volume > threshold
    
    return alert, today_volume, threshold


def S6a_d_freq_change(df, root_address,interacting_address, num_std_dev=num_std_dev_S6a_d):
    try:
        logger.info("processing S6a_d_freq_change..")
        """
        Checks if the change in daily transaction frequency for a given address is more than a specified number of standard deviations.

        Parameters:
        df (pandas.DataFrame): A DataFrame containing transaction data. Expected columns are 
                            'root_address', 'blockTimestamp', and 'historicalUSD'.
        address (str): The address to analyze.
        num_std_dev (int, optional): The number of standard deviations to use as a threshold for change in frequency. 
                                    Defaults to 2.

        Returns:
        bool: True if the change in daily transaction frequency on the last day is greater than num_std_dev standard deviations 
            from the mean frequency, else False.

        The function first filters the dataframe based on the given address and transactions with 'historicalUSD' greater than 100.
        It then groups the data by date and calculates the daily frequency of transactions. The daily frequency series is reindexed 
        to include all dates between the minimum and maximum dates in the data, filling in missing dates with zero frequency.
        The function then calculates the mean and standard deviation of the frequency, and the absolute change in frequency from the
        previous day. It checks if the change in frequency on the last day is greater than num_std_dev standard deviations from the mean 
        frequency and returns the result.
        """

        # Filter transactions based on root_address, interacting_address, and USD value
        df = df[
            (((df['root_address'] == root_address) & (df['interacting_address'] == interacting_address)) |
            ((df['root_address'] == interacting_address) & (df['interacting_address'] == root_address))) &
            (df['historicalUSD'] > 100)
        ].drop_duplicates(subset=['transactionHash'])

        # Check if the DataFrame is empty
        if df.empty:
            logger.warning("No transactions found for the specified addresses with 'historicalUSD' greater than 100.")
            return False, None, None, None  # Adjust the return values as needed
        
        df['blockTimestamp'] = pd.to_datetime(df['blockTimestamp'])
        # Get the daily transaction frequency
        daily_freq = df.groupby(df['blockTimestamp'].dt.date).size()
        
    # Create a date range from min to max date
        idx = pd.date_range(start=df['blockTimestamp'].min().date(), end=df['blockTimestamp'].max().date())

        # Reindex the daily_freq series to include all days and fill missing values with 0
        daily_freq = daily_freq.reindex(idx, fill_value=0)

        # Convert index back to datetime format
        daily_freq.index = pd.to_datetime(daily_freq.index)

        # Calculate the standard deviation of transaction frequency
        freq_mean = np.mean(daily_freq)
        freq_std = np.std(daily_freq)

        # Calculate the change in daily transaction frequency
        freq_change = daily_freq.diff().abs()

        # Compare the change on the last day with the mean and standard deviation
        last_day_change = freq_change.iloc[-1]

        # Check if the transaction frequency on the last day is less than 10
        if daily_freq.iloc[-1] < 10:
            return False, last_day_change, freq_mean, freq_std

 
        # Return True if the change in frequency is more than one standard deviation, else False
        flag = abs(last_day_change - freq_mean) > num_std_dev * freq_std

        return flag, last_day_change, freq_mean, freq_std
    except Exception as e:
            logger.error(f"An error occurred in S6a_d_freq_change: {e}")
            return None, None, None, None  # Returning None for each output in case of an error

def S6b_w_freq_change(df, root_address, interacting_address, num_std_dev=num_std_dev_S6b_w):
    try:
        logger.info("processing S6b_w_freq_change..")
        """
        Checks if the change in weekly transaction frequency for a given address is more than a specified number of standard deviations.

        Parameters:
        df (pandas.DataFrame): A DataFrame containing transaction data. Expected columns are 
                            'root_address', 'blockTimestamp', and 'historicalUSD'.
        address (str): The address to analyze.
        num_std_dev (int, optional): The number of standard deviations to use as a threshold for change in frequency. 
                                    Defaults to 2.

        Returns:
        bool: True if the change in weekly transaction frequency on the last day is greater than num_std_dev standard deviations 
            from the mean frequency, else False.

        The function first filters the dataframe based on the given address and transactions with 'historicalUSD' greater than 100.
        It then groups the data by date and calculates the weekly frequency of transactions. The weekly frequency series is reindexed 
        to include all dates between the minimum and maximum dates in the data, filling in missing dates with zero frequency.
        The function then calculates the mean and standard deviation of the frequency, and the absolute change in frequency from the
        previous day. It checks if the change in frequency on the last day is greater than num_std_dev standard deviations from the mean 
        frequency and returns the result.
        """

        # Filter transactions based on root_address, interacting_address, and USD value
        df = df[
            (((df['root_address'] == root_address) & (df['interacting_address'] == interacting_address)) |
            ((df['root_address'] == interacting_address) & (df['interacting_address'] == root_address))) &
            (df['historicalUSD'] > 100)
        ].drop_duplicates(subset=['transactionHash'])

        # Check if the DataFrame is empty
        if df.empty:
            logger.warning("No transactions found for the specified addresses with 'historicalUSD' greater than 100.")
            return False, None, None, None  # Adjust the return values as needed
        
        df['blockTimestamp'] = pd.to_datetime(df['blockTimestamp'])
        # Get the daily transaction frequency
        weekly_freq = df.groupby(df['blockTimestamp'].dt.date).size()
        
        # Create a date range from min to max date
        idx = pd.date_range(start=df['blockTimestamp'].min().date(), end=df['blockTimestamp'].max().date())

        # Reindex the weekly_freq series to include all days and fill missing values with 0
        weekly_freq = weekly_freq.reindex(idx, fill_value=0)

        # Convert index back to datetime format
        weekly_freq.index = pd.to_datetime(weekly_freq.index)

        # Group by week and get the sum for each week
        weekly_freq = weekly_freq.resample('W').sum()

        # Calculate the standard deviation of transaction frequency
        freq_mean = np.mean(weekly_freq)
        freq_std = np.std(weekly_freq)

        # Calculate the change in weekly transaction frequency
        freq_change = weekly_freq.diff().abs()

        # Compare the change on the last week with the mean and standard deviation
        last_week_change = freq_change.iloc[-1]

        # Check if the transaction frequency on the last day is less than 10
        if weekly_freq.iloc[-1] < 10:
            return False, last_week_change, freq_mean, freq_std
        
        # Return True if the change in frequency is more than num_std_dev standard deviation, else False
        flag = abs(last_week_change - freq_mean) > num_std_dev * freq_std

        return flag, last_week_change, freq_mean, freq_std
    except Exception as e:
            logger.error(f"An error occurred in S6b_w_freq_change: {e}")
            return None, None, None, None  # Returning None for each output in case of an error
#logger.info(S6b_w_freq_change(df_all_transactions, entity_root))



def S7_protocol_activity(tx, df, perc_threshold=0.2):
    try:

        logger.info("processing S7_protocol_activity..")
        tx_address = tx['root_address']
        tx_hash = tx['transactionHash']
        current_balances = eval(tx['protocol_balances']) if isinstance(tx['protocol_balances'], str) else tx['protocol_balances']

        # Get the most recent transaction prior to this one for the same address
        prev_tx = df[df['root_address'] == tx_address].sort_values(by='blockTimestamp', ascending=False).iloc[0]

        # Get the most recent transaction prior to this one for the same address, ignoring the current transaction
        prev_tx = df[(df['root_address'] == tx_address) & (df['transactionHash'] != tx_hash)].sort_values(by='blockTimestamp', ascending=False).iloc[0]
        
        prev_balances_str = prev_tx['protocol_balances']
        prev_balances = eval(prev_balances_str) if isinstance(prev_balances_str, str) else prev_balances_str

        # If prev_balances is None, initialize it as an empty dictionary
        if prev_balances is None:
            prev_balances = {}

        # Calculate the difference in balances for each protocol, in terms of percentage
        changes = {}
        for protocol in current_balances:
            old_balance = prev_balances.get(protocol, 0)
            new_balance = current_balances[protocol]
            if old_balance == 0 and new_balance != 0:
                changes[protocol] = 1
            elif old_balance != 0:
                changes[protocol] = ((new_balance - old_balance) / old_balance) 

        # Check if there is a change of 20% or more, and return a boolean indicating this
        significant_changes = {protocol: change for protocol, change in changes.items() if abs(change) >= perc_threshold}
        
        return bool(significant_changes), significant_changes, prev_balances, current_balances
    except Exception as e:
        # Log the exception and return a default value or re-raise the exception
        logger.error(f"An error occurred while processing S7_protocol_activity: {e}")
        # You can return a default value here, for example:
        return False, {}, {}, {}  # Or choose an appropriate default value based on your use case



def S8_LP_token_traded(tx):
    try:
        logger.info("processing S8_LP_token_traded..")
        # Check if 'LP' is contained in 'tokenSymbol' or 'tokenName' (case-insensitive)
        contains_lp_symbol = 'LP' in tx.get('tokenSymbol', '').upper()
        contains_lp_name = 'LP' in tx.get('tokenName', '').upper()

        # Check if any condition is True
        if contains_lp_symbol:
            return True, tx['tokenSymbol']
        elif contains_lp_name:
            return True, tx['tokenName']
        else:
            return False, None
    except Exception as e:
        logger.error(f"Error in S8_LP_token_traded: {e}")
        return False, None
