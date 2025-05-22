import requests
import pandas as pd
import requests
import time
import numpy as np
from settings import get_config, headers, url_transfers, threshold_exchange_deposit_USD_amount, limit, get_address_history_url, url_debank_complex_protocol, no_of_days_ago_s1, usd_threshold
from dateutil.parser import parse
from dateutil.tz import tzutc

import csv
import os
import logging
logger = logging.getLogger(__name__)
import pickle
from logging.handlers import RotatingFileHandler
from logging import getLogger, INFO, WARNING

config_values = get_config()
api_key = config_values['api_key']
api_key_debank = config_values['api_key_debank']



def process_exchange_deposit(interacting_addresses, filter_from_date):
    previous_senders = {}
    #limit = 1000


    for interacting_address in interacting_addresses:
        offset = 0
        senders_for_this_address = {}  # Separate dictionary for each interacting_address
        sender_transaction_counts = {}  # Store transaction counts for each sender
        sender_first_transaction_values = {}  # Store the value of the first transaction for each sender
        sender_historical_usd_values = {} #store the value of the historicalUSD

        logger.info(f"Processing address: {interacting_address}")  # print the address being processed

        while True:
            params = {
                'base': interacting_address,
                'flow': 'in',
                'limit': limit,
                'offset': offset,
                'timeGte': convert_date_to_unix_milliseconds(filter_from_date)
            }

            response = requests.get(url_transfers, params=params, headers=headers)
            if response.status_code == 200:
                transfers = response.json().get('transfers', [])
            
                for transfer in transfers:
                    sender_address = transfer['fromAddress']['address']
                    arkhamEntityType = transfer['fromAddress'].get('arkhamEntity', {}).get('type')
                    arkhamEntityName = transfer['fromAddress'].get('arkhamEntity', {}).get('name')
                    fromIsContract = transfer.get('fromIsContract', False)
                    historicalUSD = transfer.get('historicalUSD', None)
                        # Skip the iteration if fromAddress.address equals to '0x0000000000000000000000000000000000000000'
                    if sender_address == '0x0000000000000000000000000000000000000000':
                        continue

                    if (arkhamEntityName is None and arkhamEntityType not in ['cex', 'smart-contract-platform', 'miner-validator'] and not fromIsContract and historicalUSD is not None and historicalUSD > usd_threshold): #threshold_exchange_deposit_USD_amount ADD THIS AGAIN!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
                        #logger.info(transfer)
                        sender_address = transfer['fromAddress']['address']
                        transaction_hash = transfer['transactionHash']

                         # Store historicalUSD values for this sender
                        if sender_address not in sender_historical_usd_values:
                            sender_historical_usd_values[sender_address] = []
                        sender_historical_usd_values[sender_address].append(historicalUSD)

                        # Track the number of transactions for this sender
                        sender_transaction_counts[sender_address] = sender_transaction_counts.get(sender_address, 0) + 1

                        # Track the value of the first transaction for this sender
                        if sender_address not in sender_first_transaction_values:
                            sender_first_transaction_values[sender_address] = historicalUSD

                        # We store the sender's address and corresponding transaction hash in the dictionary
                        senders_for_this_address[sender_address] = transaction_hash

                count = response.json().get('count', 0)
                offset += limit

                if len(transfers) < limit or offset >= count:
                    break
            else:
                logger.info('Error: %s', response.content.decode('utf-8'))
                break

            time.sleep(0.25)

        # Filter out senders based on the given criteria
        for sender, txn_count in sender_transaction_counts.items():
            logger.info(f"Evaluating sender: {sender}")
            # logger.info(f"Transaction count for sender {sender}: {txn_count}")
            # logger.info(f"First transaction value for sender {sender}: {sender_first_transaction_values[sender]}")
            
            if txn_count < 3 and sender_first_transaction_values[sender] < 1000:
                logger.info(f"Sender {sender} meets removal criteria (txn_count < 3 and first transaction value < 1000).")
                if sender in senders_for_this_address:
                    logger.info(f"Removing sender {sender} from senders_for_this_address.")
                    del senders_for_this_address[sender]
                else:
                    logger.info(f"Sender {sender} not found in senders_for_this_address. Not removing.")
            else:
                logger.info(f"Sender {sender} does not meet removal criteria.")



# ##this is the code to add the logic of RUben. However, true positives are filtered out. 
#         # After filtering based on txn_count and sender_first_transaction_values
#         for sender, values in sender_historical_usd_values.items():
#             # Log details for each sender
#             logger.info(f"Evaluating sender {sender} for historicalUSD values.")

#             # Check which transactions have values below the threshold
#             transactions_below_threshold = [v for v in values if v <= threshold_exchange_deposit_USD_amount]
            
#             # Log the count of such transactions and their specific values
#             logger.info(f"Sender {sender} has {len(transactions_below_threshold)} transactions with values <= {threshold_exchange_deposit_USD_amount}. Transactions values: {transactions_below_threshold}")

#             if transactions_below_threshold:
#                 logger.info(f"Removing sender {sender} from senders_for_this_address due to transactions below threshold.")
#                 senders_for_this_address.pop(sender, None)
#             else:
#                 logger.info(f"Sender {sender} meets the threshold criteria and remains in senders_for_this_address.")




        logger.info(f"All previous sender addresses for exchange deposit {interacting_address}: {senders_for_this_address}")
        previous_senders[interacting_address] = senders_for_this_address  # Store the senders for this address

    original_count = len(previous_senders)
    return_limit = 200
    if original_count > return_limit:
        logger.info(f"Original count of interacting_addresses: {original_count}. Limiting to {return_limit}.")
        return dict(list(previous_senders.items())[:return_limit])
    else:
        return previous_senders
    
# def process_exchange_deposit(interacting_addresses, filter_from_date):
#     previous_senders = {}
#     #limit = 1000


#     for interacting_address in interacting_addresses:
#         offset = 0
#         senders_for_this_address = {}  # Separate dictionary for each interacting_address
#         logger.info(f"Processing address: {interacting_address}")  # print the address being processed

#         while True:
#             params = {
#                 'base': interacting_address,
#                 'flow': 'in',
#                 'limit': limit,
#                 'offset': offset,
#                 'timeGte': convert_date_to_unix_milliseconds(filter_from_date)
#             }

#             response = requests.get(url_transfers, params=params, headers=headers)
#             if response.status_code == 200:
#                 transfers = response.json().get('transfers', [])
            
#                 for transfer in transfers:
#                     sender_address = transfer['fromAddress']['address']
#                     arkhamEntityType = transfer['fromAddress'].get('arkhamEntity', {}).get('type')
#                     arkhamEntityName = transfer['fromAddress'].get('arkhamEntity', {}).get('name')
#                     fromIsContract = transfer.get('fromIsContract', False)
#                     historicalUSD = transfer.get('historicalUSD', None)
#                         # Skip the iteration if fromAddress.address equals to '0x0000000000000000000000000000000000000000'
#                     if sender_address == '0x0000000000000000000000000000000000000000':
#                         continue

#                     if (arkhamEntityName is None and arkhamEntityType not in ['cex', 'smart-contract-platform', 'miner-validator'] and not fromIsContract and historicalUSD is not None and historicalUSD > usd_threshold): 
#                         #logger.info(transfer)
#                         sender_address = transfer['fromAddress']['address']
#                         transaction_hash = transfer['transactionHash']
#                         # We store the sender's address and corresponding transaction hash in the dictionary
#                         senders_for_this_address[sender_address] = transaction_hash

#                 count = response.json().get('count', 0)
#                 offset += limit

#                 if len(transfers) < limit or offset >= count:
#                     break
#             else:
#                 logger.info('Error: %s', response.content.decode('utf-8'))
#                 break

#             time.sleep(0.25)

#         logger.info(f"All previous sender addresses for exchange deposit {interacting_address}: {senders_for_this_address}")
#         previous_senders[interacting_address] = senders_for_this_address  # Store the senders for this address

#     original_count = len(previous_senders)
#     return_limit = 200
#     if original_count > return_limit:
#         logger.info(f"Original count of interacting_addresses: {original_count}. Limiting to {return_limit}.")
#         return dict(list(previous_senders.items())[:return_limit])
#     else:
#         return previous_senders


def add_helper_columns(entity_name,df_new_txs, entity_root,get_protocol_balance=False):
    # Add "root_address" column and set to current address
    df_new_txs['root_address'] = entity_root

    #add the entity_name
    df_new_txs['entity_name'] = entity_name

    try:
    # Convert 'blockTimestamp' to datetime
        df_new_txs['blockTimestamp'] = pd.to_datetime(df_new_txs['blockTimestamp'])
    except KeyError:
        logger.info("Column 'blockTimestamp' not found. Setting it to NaN.")
        df_new_txs['blockTimestamp'] = np.nan
    except Exception as e:
        logger.info(f"Error while converting 'blockTimestamp' to datetime: {e}")


    # Add 'tx_direction' column
    df_new_txs['tx_direction'] = df_new_txs.apply(
        lambda row: 'IN' if row['toAddress.address'] == entity_root else 'OUT'
        if row['fromAddress.address'] == entity_root else 'UNKNOWN',
        axis=1
    )

    # Add 'interacting_address' column
    df_new_txs['interacting_address'] = df_new_txs['toAddress.address'].where( ((df_new_txs['toAddress.address'] != entity_root) & (df_new_txs['tx_direction'] == 'OUT')), np.nan)
    df_new_txs['interacting_address'] = df_new_txs['fromAddress.address'].where( ((df_new_txs['fromAddress.address'] != entity_root) & (df_new_txs['tx_direction'] == 'IN')),df_new_txs['interacting_address'])


    #BEGIN#################add to and from name labels###################
    # Check if columns exist in dataframe, if not create them as columns with NaN values
    if 'toAddress.arkhamEntity.name' not in df_new_txs.columns:
        df_new_txs['toAddress.arkhamEntity.name'] = np.nan
    if 'toAddress.arkhamLabel.name' not in df_new_txs.columns:
        df_new_txs['toAddress.arkhamLabel.name'] = np.nan
    if 'fromAddress.arkhamEntity.name' not in df_new_txs.columns:
        df_new_txs['fromAddress.arkhamEntity.name'] = np.nan
    if 'fromAddress.arkhamLabel.name' not in df_new_txs.columns:
        df_new_txs['fromAddress.arkhamLabel.name'] = np.nan

    # Now you can safely concatenate or add them together
    df_new_txs['to_name_label'] = df_new_txs['toAddress.arkhamEntity.name'].astype(str).fillna('') + ' (' + df_new_txs['toAddress.arkhamLabel.name'].fillna('') + ')'
    df_new_txs['from_name_label'] = df_new_txs['fromAddress.arkhamEntity.name'].astype(str).fillna('') + ' (' + df_new_txs['fromAddress.arkhamLabel.name'].fillna('') + ')'

    # Replace ' ()' with NaN
    df_new_txs['to_name_label'] = df_new_txs['to_name_label'].astype(str).replace(' ()', np.nan)
    df_new_txs['from_name_label'] = df_new_txs['from_name_label'].astype(str).replace(' ()', np.nan)

    # If the string starts with '(' and ends with ')', remove the parentheses
    df_new_txs['to_name_label'] = df_new_txs['to_name_label'].astype(str).apply(lambda x: x[1:-1] if isinstance(x, str) and x.strip().startswith('(') and x.strip().endswith(')') else x)
    df_new_txs['from_name_label'] = df_new_txs['from_name_label'].astype(str).apply(lambda x: x[1:-1] if isinstance(x, str) and x.strip().startswith('(') and x.strip().endswith(')') else x)

    # If the string ends with ' ()', remove the ' ()'
    df_new_txs['to_name_label'] = df_new_txs['to_name_label'].astype(str).apply(lambda x: x[:-3] if isinstance(x, str) and x.strip().endswith(' ()') else x)
    df_new_txs['from_name_label'] = df_new_txs['from_name_label'].astype(str).apply(lambda x: x[:-3] if isinstance(x, str) and x.strip().endswith(' ()') else x)

    # If the string still starts with '(', remove the '('
    df_new_txs['to_name_label'] = df_new_txs['to_name_label'].astype(str).apply(lambda x: x.lstrip('(') if isinstance(x, str) else x)
    df_new_txs['from_name_label'] = df_new_txs['from_name_label'].astype(str).apply(lambda x: x.lstrip('(') if isinstance(x, str) else x)

    # Add 'protocol_balances' column
    if get_protocol_balance:
        df_new_txs['protocol_balances'] = None
        try:
            for idx, row in df_new_txs.iterrows():
                balances = get_protocol_balances(entity_root)
                df_new_txs.at[idx, 'protocol_balances'] = str(balances)
        except Exception as e:
            logger.info(f"Error getting balances for address {entity_root}: {e}")

    #END#################add to and from name labels###################

    return df_new_txs



def get_extra_transactions(entity_name, previous_senders,filter_from_date):
    df_all_extra_transactions = pd.DataFrame()

     # Get the list of sender addresses (inner keys) (Coming from the exchange deposit identification method)
    if isinstance(previous_senders, dict):
        sender_addresses = list(set([address for sub_dict in previous_senders.values() for address in sub_dict.keys()]))

    #get the list of addresses coming from the signalised identification method
    elif isinstance(previous_senders, set):
        sender_addresses = list(set([tup[0] for tup in previous_senders]))

    # Process no_of_addresses sender addresses at a time
    no_of_addresses = 1 #API only allows two at a time

    for i in range(0, len(sender_addresses), no_of_addresses):
        addresses_batch = ','.join(sender_addresses[i:i+no_of_addresses])  # Create comma-separated addresses string
        logger.info(f"Processing addresses: {addresses_batch}")  # print the addresses being processed

        offset = 0
        #limit = 1000
        all_transfers = []  # Initialize all_transfers here

        while True:
            params = {
                'base': addresses_batch,
                'limit': limit,
                'offset': offset,
                'timeGte': convert_date_to_unix_milliseconds(filter_from_date)
            }

            try:
                response = requests.get(url_transfers, params=params, headers=headers)
            except Exception as e:
                logger.info(f"Request failed with exception: {e}")

            if response.status_code == 200:
                logger.info("succes")
                transfers = response.json().get('transfers', [])
                all_transfers.extend(transfers)
                count = response.json().get('count', 0)
                offset += limit

                # produce a status update for every processed batch
                batch_number = offset // limit
                tx_start = (batch_number - 1) * limit + 1
                tx_end = batch_number * limit
                logger.info(f"Batch {batch_number} ({tx_start}-{tx_end} txs) processed")

                if len(transfers) < limit or offset >= count:
                    break
            else:
                logger.info('Error:', response.content)
                break

            time.sleep(0.2)

        df_extra_transactions = pd.json_normalize(all_transfers)


        # add the extra columns to the dataframe
        for address in addresses_batch.split(','):
            
            #filter out the null addresses
            #if address == '0x0000000000000000000000000000000000000000':
            #    continue

            # Check if 'toAddress.address' and 'fromAddress.address' columns exist
            if 'toAddress.address' in df_extra_transactions.columns and 'fromAddress.address' in df_extra_transactions.columns:
                # Filter the transactions for the current address and create a copy
                df_address_transactions = df_extra_transactions[(df_extra_transactions['toAddress.address'] == address) | (df_extra_transactions['fromAddress.address'] == address)].copy()

                # Process only the transactions involving the current address
                df_address_transactions = add_helper_columns(entity_name, df_address_transactions, address)

                # Concatenate the processed transactions back into the dataframe
                df_all_extra_transactions = pd.concat([df_all_extra_transactions, df_address_transactions])

                # Drop duplicates based on 'id' and 'tx_direction'
                df_all_extra_transactions.drop_duplicates(subset=['id', 'tx_direction'], keep='first', inplace=True)
            else:
                logger.warning(f"Columns 'toAddress.address' or 'fromAddress.address' not found for address {address}. Skipping this address.")
            

        #reset index
        df_all_extra_transactions.reset_index(drop=True, inplace=True)

    return df_all_extra_transactions  # return the concatenated DataFrame


def append_addresses_to_csv(address_entity_pairs, file_name='identified_addresses.csv'):
    # Determine the filename based on the environment
    if os.environ.get("RUNNING_IN_ACI"):
        file_name = '/mnt/data/identified_addresses.csv'
    else:
        file_name = 'mnt/data/identified_addresses.csv'  # Adjust this if you want a different local path

    # Initial setup for header writing flag and list of existing tuples
    write_header = False
    existing_tuples = []

    # Read existing address-entity pairs from the CSV file
    try:
        if os.path.getsize(file_name) > 0:
            existing_pairs_df = pd.read_csv(file_name)
            existing_tuples = list(existing_pairs_df.itertuples(index=False, name=None))
        else:
            write_header = True  # If file is empty, set the flag to write the header
    except FileNotFoundError:
        write_header = True  # If file not found, set the flag to write the header

    # Combine lists, replace entries with same address but different third element, and remove duplicates
    combined_addresses = []
    for new_tuple in address_entity_pairs:
        replaced = False
        for idx, existing_tuple in enumerate(existing_tuples):
            if new_tuple[0] == existing_tuple[0] and new_tuple[2] != existing_tuple[2]:
                existing_tuples[idx] = new_tuple  # Replace the tuple
                replaced = True
                break
        if not replaced:
            combined_addresses.append(new_tuple)
    
    combined_addresses = list(set(existing_tuples + combined_addresses))

    # Write the combined list back to the CSV
    with open(file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(['Address', 'Entity_Name', 'Identification_Method'])  # Write the headers to the CSV
        for address, entity, identification_method in combined_addresses:
            writer.writerow([address, entity, identification_method])


def save_addresses(all_identified_addresses,status=None):
    """
    Extracts address-entity-identification_method triples from the identified addresses 
    and appends them to a CSV file.
    """
    
  # Extract the address and entity name pairs from identified_addresses and convert them to a list
    try:
        addresses_to_save = [(address, entity, status or identification_method) 
                             for address, _, entity, identification_method in all_identified_addresses]
        logger.info(f"Extracted {len(addresses_to_save)} address-entity-identification_method triples.")
    except Exception as e:
        logger.error(f"Error while extracting address-entity-identification_method triples: {e}")
        return

    # Call the function to save addresses
    try:
        append_addresses_to_csv(addresses_to_save)
        logger.info("Successfully appended addresses")
    except Exception as e:
        logger.error(f"Error while appending addresses to the CSV: {e}")





def filter_transactions_and_addresses(df_all_transactions, identified_addresses, filter_from_date):
    """
    Filters the df_all_transactions based on blockTimestamp and updates identified_addresses list
    based on valid transactionHashes.

    Args:
    - df_all_transactions (pd.DataFrame): DataFrame containing all transactions.
    - identified_addresses (list): List of (address, transactionHash, entity_name) tuples.
    - filter_from_date (str): Date string to filter transactions from.

    Returns:
    - pd.DataFrame: Filtered df_all_transactions.
    - list: Updated identified_addresses.
    """
    
    # Convert filter_from_date to a datetime object
   # Convert filter_from_date to a timezone-aware datetime object
    filter_from_date = pd.to_datetime(filter_from_date).tz_localize('UTC')

    # Log initial counts
    initial_transaction_count = df_all_transactions.shape[0]
    initial_address_count = len(identified_addresses)

    # Filter df_all_transactions based on blockTimestamp
    df_all_transactions = df_all_transactions[df_all_transactions['blockTimestamp'] > filter_from_date]

    # Log after filtering transactions
    logger.info(f"Filtered out {initial_transaction_count - df_all_transactions.shape[0]} transactions.")
    logger.info(f"{df_all_transactions.shape[0]} transactions left after filtering.")

    # Filter identified_addresses based on transactionHashes present in df_all_transactions
    valid_transaction_hashes = set(df_all_transactions['transactionHash'])
    identified_addresses = [(address, tx_hash, entity_name, identification_method) for address, tx_hash, entity_name, identification_method in identified_addresses if tx_hash in valid_transaction_hashes]

    # Log after filtering addresses
    logger.info(f"Filtered out {initial_address_count - len(identified_addresses)} addresses.")
    logger.info(f"{len(identified_addresses)} addresses left after filtering.")

    return df_all_transactions, identified_addresses



def add_helper_columns_df(entity_name, df, root_address_col):

    df['entity_name'] = entity_name

    # Convert 'blockTimestamp' to datetime
    # Try-except block around 'blockTimestamp'
    try:
        df['blockTimestamp'] = pd.to_datetime(df['blockTimestamp'])
    except KeyError:
        logger.info("Column 'blockTimestamp' not found. Setting it to NaN.")
        df['blockTimestamp'] = np.nan
    except Exception as e:
        logger.info(f"Error while converting 'blockTimestamp' to datetime: {e}")
    
    
    # Add 'tx_direction' column
    df['tx_direction'] = np.where(df['toAddress.address'] == df[root_address_col], 'IN',
                                  np.where(df['fromAddress.address'] == df[root_address_col], 'OUT', 'UNKNOWN'))
    
    # Add 'interacting_address' column
    df['interacting_address'] = np.where((df['toAddress.address'] != df[root_address_col]) & (df['tx_direction'] == 'OUT'), 
                                         df['toAddress.address'], 
                                         np.where((df['fromAddress.address'] != df[root_address_col]) & (df['tx_direction'] == 'IN'), 
                                                  df['fromAddress.address'], np.nan))
    
    # Process name labels
    df['to_name_label'] = df['toAddress.arkhamEntity.name'].fillna('') + ' (' + df['toAddress.arkhamLabel.name'].fillna('') + ')'
    df['from_name_label'] = df['fromAddress.arkhamEntity.name'].fillna('') + ' (' + df['fromAddress.arkhamLabel.name'].fillna('') + ')'
    
    # Replace ' ()' with NaN
    df['to_name_label'] = df['to_name_label'].replace(' ()', np.nan)
    df['from_name_label'] = df['from_name_label'].replace(' ()', np.nan)

    # If the string starts with '(' and ends with ')', remove the parentheses
    mask_to = df['to_name_label'].str.startswith('(') & df['to_name_label'].str.endswith(')')
    mask_from = df['from_name_label'].str.startswith('(') & df['from_name_label'].str.endswith(')')
    df.loc[mask_to, 'to_name_label'] = df['to_name_label'].str[1:-1]
    df.loc[mask_from, 'from_name_label'] = df['from_name_label'].str[1:-1]
    
    # If the string ends with ' ()', remove the ' ()'
    df['to_name_label'] = np.where(df['to_name_label'].str.endswith(' ()'), df['to_name_label'].str[:-3], df['to_name_label'])
    df['from_name_label'] = np.where(df['from_name_label'].str.endswith(' ()'), df['from_name_label'].str[:-3], df['from_name_label'])
    
    # If the string still starts with '(', remove the '('
    df['to_name_label'] = df['to_name_label'].str.lstrip('(')
    df['from_name_label'] = df['from_name_label'].str.lstrip('(')
   

    return df


def get_entity_transfers(entity_name, arkham_entity, limit=1000):
    logger.info(f"Starting to fetch transfers for entity: {arkham_entity}")
    
    all_transfers = []
    offset = 0
    identified_addresses_entity_call = []

    while True:
        params = {
            'base': arkham_entity,
            'limit': limit,
            'offset': offset
        }

        response = requests.get(url_transfers, params=params, headers=headers)

        if response.status_code == 200:
            transfers = response.json().get('transfers', [])
            all_transfers.extend(transfers)

            for transfer in transfers:
                from_address_data = transfer.get('fromAddress', {})
                from_address = from_address_data.get('address', '')
                from_address_entity_id = from_address_data.get('arkhamEntity', {}).get('id', '')

                to_address_data = transfer.get('toAddress', {})
                to_address = to_address_data.get('address', '')
                to_address_entity_id = to_address_data.get('arkhamEntity', {}).get('id', '')

                transaction_hash = transfer.get('transactionHash', '')

                if arkham_entity.lower() in [from_address_entity_id.lower(), to_address_entity_id.lower()]:
                    matched_address = from_address if arkham_entity.lower() == from_address_entity_id.lower() else to_address
                    identified_addresses_entity_call.append((matched_address, transaction_hash))

                    
                    # Add root_address to the transfer
                    transfer['root_address'] = matched_address


            count = response.json().get('count', 0)
            offset += limit

            # Produce a status update for every processed batch
            batch_number = offset // limit
            tx_start = (batch_number - 1) * limit + 1
            tx_end = batch_number * limit
            logger.info(f"Batch {batch_number} ({tx_start}-{tx_end} txs) processed")

            if len(transfers) < limit or offset >= count:
                break
        else:
            logger.error('Error:', response.content)
            break

        time.sleep(0.2)

    df_entity_tx = pd.json_normalize(all_transfers)


    #add extra columns
    df_entity_tx_enriched  = add_helper_columns_df(entity_name, df_entity_tx,'root_address')

    #drop duplicates based on address and keep the most recently added element
    identified_addresses_entity_call = list({addr: (addr, tx_hash) for addr, tx_hash in reversed(identified_addresses_entity_call)}.values())


    logger.info(f"Total transactions fetched: {df_entity_tx_enriched.shape[0]}")
    logger.info(f"Total addresses identified: {len(identified_addresses_entity_call)}")


    return df_entity_tx_enriched, identified_addresses_entity_call



def add_known_entity_transactions(df_all_transactions, identified_addresses, entity_name, arkham_entity):
    try:
        logger.info(f"Starting the add_known_entity_transactions function with arkham_entity: {arkham_entity}")
        #add all transactions with known Arkham Label (e.g. Portofino Technologies) to df_all_transactions and the addresses and transactionHashes to identified_addresses
        df_entity, addresses_list = get_entity_transfers(entity_name, arkham_entity)
        df_all_transactions = pd.concat([df_all_transactions, df_entity], ignore_index=True)
        logger.info(f"Concatenated transactions. New df_all_transactions size: {len(df_all_transactions)}")

        df_all_transactions.drop_duplicates(subset='id', inplace=True)
        logger.info(f"Removed duplicates. df_all_transactions size after deduplication: {len(df_all_transactions)}")

        logger.info(f"Length identified addresses: {len(identified_addresses)}")
        logger.info(f"Length addresses list: {len(addresses_list)}")

        # Appending the entity_name to the addresses_list tuples
        extended_addresses_list = [(address, tx_hash, entity_name,"entity_call_method") for address, tx_hash in addresses_list]
        identified_addresses.extend(extended_addresses_list)
        logger.info(f"Extended identified_addresses. New size: {len(identified_addresses)}")

        # To drop duplicates based on 'address':
        seen_addresses = set()
        identified_addresses = [(address, tx_hash,entity, identification_method) for address, tx_hash, entity, identification_method in identified_addresses if address not in seen_addresses and not seen_addresses.add(address)]
        logger.info(f"Removed duplicate addresses. New identified_addresses size: {len(identified_addresses)}")


    except Exception as e:
        # This will print the type, value, and traceback of the exception.
        # Adjust this based on how you'd like to handle the exception.
        logger.error(f"An error occurred: {type(e).__name__} - {str(e)}")

    return df_all_transactions, identified_addresses




def find_entity_by_name(entities, entity_name):
    """Retrieve entity by name from a list of entities."""
    for entity in entities:
        if entity['entity_name'] == entity_name:
            return entity
    return None



def convert_date_to_unix_milliseconds(date_input):
    """
    Converts a date string or Timestamp object to Unix millisecond timestamp.

    Parameters:
    - date_input (str, int, or Timestamp): The date string, Unix timestamp, or Timestamp object to convert.

    Returns:
    - int: The Unix millisecond timestamp.
    """
    #logging.info(f"Received input: {date_input} of type {type(date_input)}")

    if isinstance(date_input, str):
    #    logging.info("Input is a string. Converting to datetime object.")
        dt_obj = pd.to_datetime(date_input)
        unix_milliseconds = int(dt_obj.timestamp() * 1000)
    #    logging.info("Converting done.")
    elif isinstance(date_input, pd.Timestamp):
    #    logging.info("Input is a Timestamp object. Converting to Unix milliseconds.")
        unix_milliseconds = int(date_input.timestamp() * 1000)
    #    logging.info("Converting done.")
    else:
        logging.info("Input is neither a string nor a Timestamp. Assuming it's already in the correct format.")
        # Assume it's already in the correct format if it's not a string or Timestamp
        unix_milliseconds = int(date_input)
    #logging.info(f"Converted input to Unix milliseconds: {unix_milliseconds}")
    return unix_milliseconds




def load_state():
    if os.environ.get("RUNNING_IN_ACI"):  # You set this environment variable only in your Azure container
        addresses_filename = '/mnt/data/all_identified_addresses.pkl'
        transactions_filename = '/mnt/data/all_transactions_df.pkl'
    else:
        addresses_filename = 'mnt/data/all_identified_addresses.pkl'  # Replace with appropriate local path
        transactions_filename = 'mnt/data/all_transactions_df.pkl'  # Replace with appropriate local path

    logger.info(f"Filenames determined: {addresses_filename}, {transactions_filename}")

    try:
        with open(addresses_filename, 'rb') as f:
            all_identified_addresses = pickle.load(f)
        with open(transactions_filename, 'rb') as g:
            all_transactions_df = pickle.load(g)
        # all_transactions_df = pd.read_csv(transactions_filename)
        logger.info(f"Successfully loaded state from {addresses_filename} and {transactions_filename}.")
        return all_transactions_df, all_identified_addresses
    except (FileNotFoundError, IOError) as e:
        logger.error(f"Failed to load saved state: {e}")
        return None, None  # Return None to indicate failure to load state


def append_state(all_transactions_df, all_identified_addresses):
    logger.info("Appending state...")

    # Determine filenames based on environment
    if os.environ.get("RUNNING_IN_ACI"):
        addresses_filename = '/mnt/data/all_identified_addresses.pkl'
        transactions_filename = '/mnt/data/all_transactions_df.pkl'
    else:
        addresses_filename = 'mnt/data/all_identified_addresses.pkl'  # Replace with appropriate local path
        transactions_filename = 'mnt/data/all_transactions_df.pkl'  # Replace with appropriate local path

    logger.info(f"Filenames determined: {addresses_filename}, {transactions_filename}")

    # Append and save all_identified_addresses
    try:
        if os.path.exists(addresses_filename):
            with open(addresses_filename, 'rb') as f:
                existing_addresses = pickle.load(f)
            combined_addresses = list(set(existing_addresses + all_identified_addresses))
        else:
            combined_addresses = all_identified_addresses
        
        unique_addresses = {t[0]: t for t in combined_addresses}.values()
        combined_addresses = list(unique_addresses)

        with open(addresses_filename, 'wb') as f:
            pickle.dump(combined_addresses, f)
        logger.info(f"Successfully saved all_identified_addresses to {addresses_filename}.")
    except Exception as e:
        logger.error(f"Error while saving all_identified_addresses to {addresses_filename}: {e}")

    # Append and save all_transactions_df
    try:
        if os.path.exists(transactions_filename):
            with open(transactions_filename, 'rb') as f:
                existing_transactions_df = pd.read_pickle(f)
            combined_transactions_df = pd.concat([existing_transactions_df, all_transactions_df]).drop_duplicates(subset=['id', 'transactionHash','root_address'])
        else:
            combined_transactions_df = all_transactions_df

        with open(transactions_filename, 'wb') as f:
            pickle.dump(combined_transactions_df, f)
        logger.info(f"Successfully appended all_transactions_df to {transactions_filename}.")
    except Exception as e:
        logger.error(f"Error while appending all_transactions_df to {transactions_filename}: {e}")

def save_state(all_transactions_df, all_identified_addresses):
    logger.info("Saving state...")

    if os.environ.get("RUNNING_IN_ACI"):  # You set this environment variable only in your Azure container
        addresses_filename = '/mnt/data/all_identified_addresses.pkl'
        transactions_filename = '/mnt/data/all_transactions_df.pkl'
    else:
        addresses_filename = 'mnt/data/all_identified_addresses.pkl'  # Replace with appropriate local path
        transactions_filename = 'mnt/data/all_transactions_df.pkl'  # Replace with appropriate local path

    logger.info(f"Filenames determined: {addresses_filename}, {transactions_filename}")

    # Save all_identified_addresses to a pickle file
    try:
        with open(addresses_filename, 'wb') as f:
            pickle.dump(all_identified_addresses, f)
        logger.info(f"Successfully saved all_identified_addresses to {addresses_filename}.")
    except Exception as e:
        logger.error(f"Error while saving all_identified_addresses to {addresses_filename}: {e}")

    # Check if the transactions file exists
    transactions_file_exists = os.path.isfile(transactions_filename)
    if transactions_file_exists:
        logger.info(f"File {transactions_filename} exists. Overwriting the existing file.")
    else:
        logger.info(f"File {transactions_filename} does not exist. A new file will be created.")

    # Save all_transactions_df to a CSV file
    try:
        #all_transactions_df.to_csv(transactions_filename, index=False)
        with open(transactions_filename, 'wb') as f:
            pickle.dump(all_transactions_df, f)
        logger.info(f"Successfully saved all_transactions_df to {transactions_filename}.")
    except Exception as e:
        logger.error(f"Error while saving all_transactions_df to {transactions_filename}: {e}")



def get_usd_balance(address):
    # Make sure necessary variables like api_key are globally available or defined elsewhere
    headers = {"API-Key": api_key}

    # Send a GET request to the API for history
    response_history = requests.get(get_address_history_url(address), headers=headers)

    # Ensure the request was successful
    if response_history.status_code != 200:
        raise Exception(f"API request for history failed with status code {response_history.status_code}")

    data = response_history.json()


    # Process history data
    total_usd = 0

    for _,history in data.items():
        history = sorted(history, key=lambda x: parse(x["time"]), reverse=True)
        chain_usd = 0
        
        if history:
            chain_usd = history[0]["usd"]
            total_usd += chain_usd
            total_usd = int(total_usd)
    return total_usd




def process_signalised_address(row, signal_identified_pairs, identified_addresses, last_seen_tx_timestamps):
    """
    Processes a transaction row to identify signals and record necessary information.

    Parameters:
    - row (DataFrame): The transaction data.
    - signal_identified_pairs (set): A set to store identified pairs of interacting address and transaction hash.
    - identified_addresses (list): A list to store details of identified addresses.
    - last_seen_tx_timestamps (dict): A dictionary to store the last seen timestamps.

    Returns:
    - None
    """

    def get_value_or_none(dataframe, key):
        """Safely retrieves a value from a DataFrame, returning None if the key doesn't exist."""
        return dataframe.get(key) if key in dataframe else None

    def is_nan_or_none(value):
        """Checks if a value is NaN or None."""
        return pd.isna(value) or value is None

    try:
        logger.info("Processing signalised address row...")
        if (row['tx_direction'] == 'OUT' and 
            is_nan_or_none(get_value_or_none(row, 'toAddress.arkhamEntity.name')) and 
            is_nan_or_none(get_value_or_none(row, 'toAddress.arkhamEntity.type'))) or \
           (row['tx_direction'] == 'IN' and 
            is_nan_or_none(get_value_or_none(row, 'fromAddress.arkhamEntity.name')) and 
            is_nan_or_none(get_value_or_none(row, 'fromAddress.arkhamEntity.type'))):

            interacting_address = row['interacting_address']
            transaction_hash = row['transactionHash']
            entity_name = row['entity_name']

            # Add interacting address and transactionHash pair to the set
            signal_identified_pairs.add((interacting_address,transaction_hash))
            logger.info(f"Added to signal_identified_pairs: {interacting_address}, {transaction_hash}")

            # Add tuple to identified_addresses
            identified_addresses.append((interacting_address,transaction_hash, entity_name, "signalised_identification_method"))
            logger.info(f"Added to identified_addresses: {interacting_address}, {transaction_hash}")

            # Add timestamp to list
            last_seen_tx_timestamps[interacting_address] = convert_date_to_unix_milliseconds(row['blockTimestamp'])
    except Exception as e:  # Catching all exceptions can help catch unexpected errors and log them.
        logger.info(f"An error occurred: {e}")


def get_and_log_env_variables(logger):
    env_vars = ['START_FROM_STATE', 'RUNNING_IN_ACI', 'ACTIVE_PROTOCOL_SIGNAL','CALL_SWARM']
    env_values = {}

    # Convert 'true'/'false' environment variables to actual boolean values
    for var in env_vars:
        if os.environ.get(var) is not None:
            value_str = os.environ.get(var, 'False')
            value = value_str.lower() == 'true'
            env_values[var] = value
        else:
            env_values[var] = False

    return env_values



def get_protocol_balances(address):
    # Check if the ACTIVE_PROTOCOL_SIGNAL environment variable is set to 'True'
    if os.getenv('ACTIVE_PROTOCOL_SIGNAL') == 'True':
        logger.info("ACTIVE_PROTOCOL_SIGNAL set to True. Fetching protocol balances...")
        # Define the URL for the API request
        url = url_debank_complex_protocol

        # Define the headers for the API request
        headers = {
            'accept': 'application/json',
            'AccessKey': api_key_debank
        }

        # Define the parameters for the API request
        params = {
            'id': address
        }

        # Make the API request and get the response
        response = requests.get(url, headers=headers, params=params)
        
        # If the request was successful, parse the response and return the balances
        if response.status_code == 200:
            data = response.json()

            # Create a dictionary to hold the balances
            balances = {}

            # Iterate through each protocol in the response data
            for protocol in data:
                # Extract the protocol name
                protocol_name = protocol.get('name', 'unknown')

                # Extract the total balance for this protocol
                protocol_balance = 0
                for item in protocol.get('portfolio_item_list', []):
                    protocol_balance += item.get('stats', {}).get('net_usd_value', 0)
                
                # Store the balance in the dictionary
                balances[protocol_name] = protocol_balance

            # Return the balances
            return balances

        # If the request was not successful, raise an exception
        else:
            raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
    else:
        # If ACTIVE_PROTOCOL_SIGNAL is not 'True', return an empty dictionary
        #logger.info("ACTIVE_PROTOCOL_SIGNAL NOT set to True. returning empty dictonary...")
        return {}
    


def logger_setup(log_path):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    if os.environ.get("RUNNING_IN_ACI"):
        file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=3)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

     # Set the log level to WARNING for the httpx logger
    httpx_logger = getLogger("httpx")
    httpx_logger.setLevel(WARNING)

    return logger





