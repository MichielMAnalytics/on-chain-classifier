import requests
import pandas as pd
import requests
import time
import numpy as np
from dateutil.parser import parse
from variables import entities, no_of_in_interactions_threshold, no_of_out_interactions_threshold, USD_tx_threshold, pct_total_value_threshold, single_address_input
from supporting_functions import process_exchange_deposit, get_extra_transactions, convert_date_to_unix_milliseconds, save_state
from root_identification import get_transfers, extract_entity_root
from settings import get_config, headers, url_transfers, threshold_exchange_deposit, limit
config_values = get_config()
api_key = config_values['api_key']
import logging
logger = logging.getLogger(__name__)


def process_addresses(entity_name,entity_root, transaction_hash, entity_root_historicalUSD,entities_to_include, df_all_transactions, filter_from_date):
    logging.info(f"Processing address: {entity_root} for entity: {entity_name}")

    all_transfers = []
    offset = 0

    while True:
        params = {
            'base': entity_root,
            'limit': limit,
            'offset': offset,
            'timeGte': convert_date_to_unix_milliseconds(filter_from_date)
        }

        response = requests.get(url_transfers, params=params, headers=headers)

        if response.status_code == 200:
            transfers = response.json().get('transfers', [])
            all_transfers.extend(transfers)
            count = response.json().get('count', 0)
            offset += limit

            #produce a status update for every processed batcg
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

    df_entity_root_tx = pd.json_normalize(all_transfers)

    #convert the timestamp to a datetime format
    try:
    # Convert 'blockTimestamp' to datetime
        df_entity_root_tx['blockTimestamp'] = pd.to_datetime(df_entity_root_tx['blockTimestamp'])
    except KeyError:
        logger.info("Column 'blockTimestamp' not found. Setting it to NaN.")
        df_entity_root_tx['blockTimestamp'] = np.nan
    except Exception as e:
        logger.info(f"Error while converting 'blockTimestamp' to datetime: {e}")



    #create columns with IN and OUT transactions
    df_entity_root_tx['tx_direction'] = df_entity_root_tx.apply(
        lambda row: 'IN' if row['toAddress.address'] == entity_root else 'OUT'
        if row['fromAddress.address'] == entity_root else 'UNKNOWN',
        axis=1
    )

    #create a column with the address that is being processed
    df_entity_root_tx['root_address'] = entity_root

    #create a column with the address the root_address is interacting with
    df_entity_root_tx['interacting_address'] = df_entity_root_tx['toAddress.address'].where( ((df_entity_root_tx['toAddress.address'] != entity_root) & (df_entity_root_tx['tx_direction'] == 'OUT')), np.nan)
    df_entity_root_tx['interacting_address'] = df_entity_root_tx['fromAddress.address'].where( ((df_entity_root_tx['fromAddress.address'] != entity_root) & (df_entity_root_tx['tx_direction'] == 'IN')),df_entity_root_tx['interacting_address']) 

    #add the manual entity name  
    df_entity_root_tx['entity_name'] = entity_name


    #BEGIN#################add to and from name labels###################
    # Check if columns exist in dataframe, if not create them as columns with NaN values
    if 'toAddress.arkhamEntity.name' not in df_entity_root_tx.columns:
        df_entity_root_tx['toAddress.arkhamEntity.name'] = np.nan
    if 'toAddress.arkhamLabel.name' not in df_entity_root_tx.columns:
        df_entity_root_tx['toAddress.arkhamLabel.name'] = np.nan
    if 'fromAddress.arkhamEntity.name' not in df_entity_root_tx.columns:
        df_entity_root_tx['fromAddress.arkhamEntity.name'] = np.nan
    if 'fromAddress.arkhamLabel.name' not in df_entity_root_tx.columns:
        df_entity_root_tx['fromAddress.arkhamLabel.name'] = np.nan

    # Now you can safely concatenate or add them together
    df_entity_root_tx['to_name_label'] = df_entity_root_tx['toAddress.arkhamEntity.name'].fillna('') + ' (' + df_entity_root_tx['toAddress.arkhamLabel.name'].fillna('') + ')'
    df_entity_root_tx['from_name_label'] = df_entity_root_tx['fromAddress.arkhamEntity.name'].fillna('') + ' (' + df_entity_root_tx['fromAddress.arkhamLabel.name'].fillna('') + ')'

    # Replace ' ()' with NaN
    df_entity_root_tx['to_name_label'] = df_entity_root_tx['to_name_label'].replace(' ()', np.nan)
    df_entity_root_tx['from_name_label'] = df_entity_root_tx['from_name_label'].replace(' ()', np.nan)

    # If the string starts with '(' and ends with ')', remove the parentheses
    df_entity_root_tx['to_name_label'] = df_entity_root_tx['to_name_label'].apply(lambda x: x[1:-1] if isinstance(x, str) and x.strip().startswith('(') and x.strip().endswith(')') else x)
    df_entity_root_tx['from_name_label'] = df_entity_root_tx['from_name_label'].apply(lambda x: x[1:-1] if isinstance(x, str) and x.strip().startswith('(') and x.strip().endswith(')') else x)

    # If the string ends with ' ()', remove the ' ()'
    df_entity_root_tx['to_name_label'] = df_entity_root_tx['to_name_label'].apply(lambda x: x[:-3] if isinstance(x, str) and x.strip().endswith(' ()') else x)
    df_entity_root_tx['from_name_label'] = df_entity_root_tx['from_name_label'].apply(lambda x: x[:-3] if isinstance(x, str) and x.strip().endswith(' ()') else x)

    # If the string still starts with '(', remove the '('
    df_entity_root_tx['to_name_label'] = df_entity_root_tx['to_name_label'].apply(lambda x: x.lstrip('(') if isinstance(x, str) else x)
    df_entity_root_tx['from_name_label'] = df_entity_root_tx['from_name_label'].apply(lambda x: x.lstrip('(') if isinstance(x, str) else x)
    #END#################add to and from name labels###################

    exchange_deposit_addresses = df_entity_root_tx[(df_entity_root_tx['tx_direction'] == 'OUT') &
                                                    (df_entity_root_tx['to_name_label'].fillna("").str.contains('deposit', case=False) &
                                                     (~df_entity_root_tx['to_name_label'].fillna("").str.contains('Copper', case=False)) & 
                                                     (~df_entity_root_tx['to_name_label'].fillna("").str.contains('DepositAndPlaceOrder', case=False)) & 
                                                    (df_entity_root_tx['historicalUSD'] > threshold_exchange_deposit) )]['interacting_address'].tolist()

  # Convert the list to a DataFrame
    df_export = pd.DataFrame({
        'interacting_address': exchange_deposit_addresses
    })


    logging.info(f"Current size of df_all_transactions: {len(df_all_transactions)}")
    logging.info(f"Appending transactions for address: {entity_root} and entity: {entity_name}")

    # Append df_entity_root_tx to df_all_transactions
    df_all_transactions = pd.concat([df_all_transactions, df_entity_root_tx], axis=0, ignore_index=True, sort=False)


    #extract the incoming and outgoing entities to include
    incoming_entities_to_include = entities_to_include['incoming_entities_to_include']
    outgoing_entities_to_include = entities_to_include['outgoing_entities_to_include']


    #filter out the other entities
    try:
        df_entity_root_tx = df_entity_root_tx[
            (df_entity_root_tx['fromAddress.arkhamEntity.name'].isna()) | 
            (df_entity_root_tx['fromAddress.arkhamEntity.name'].str.contains('|'.join(incoming_entities_to_include)))
        ]
    except Exception as e:
        logger.info("Warning: Cannot filter on 'fromAddress.arkhamEntity.name' because the column was not found")

    try:
        df_entity_root_tx = df_entity_root_tx[
            (df_entity_root_tx['fromAddress.arkhamLabel.name'].isna()) | 
            (df_entity_root_tx['fromAddress.arkhamLabel.name'].str.contains('|'.join(incoming_entities_to_include)))
        ]
    except Exception as e:
        logger.info("Warning: Cannot filter on 'fromAddress.arkhamLabel.name' because the column was not found")

    try:
        df_entity_root_tx = df_entity_root_tx[
            (df_entity_root_tx['toAddress.arkhamLabel.name'].isna()) | 
            (df_entity_root_tx['toAddress.arkhamLabel.name'].str.contains('|'.join(outgoing_entities_to_include)))
            
        ]
    except Exception as e:
        logger.info("Warning: Cannot filter on 'toAddress.arkhamLabel.name' because the column was not found")


    entity_tx_hash = transaction_hash
    root_tx_condition = df_entity_root_tx['transactionHash'] == entity_tx_hash
    df_entity_root_tx['original_tx'] = np.where(root_tx_condition, True, False)

    #get the first ETH transaction
    df_filtered = df_entity_root_tx[(df_entity_root_tx['tokenSymbol'] == 'ETH') | (df_entity_root_tx['tokenSymbol'] == 'WETH')]
    # Apply the second filter. Make sure that the toAddress is the entity_root
    df_filtered = df_filtered[df_filtered['tx_direction'] == 'IN']

    # Apply the third filter on the resulting dataframe. Take the minimum timestamp
    df_filtered = df_filtered[df_filtered['blockTimestamp'] == df_filtered['blockTimestamp'].min()]
    # # Now df_filtered contains the rows where all three conditions are met. You can create a 'first_in_tx' column in the original dataframe that is True for these rows, and False otherwise
    df_entity_root_tx['first_in_tx'] = df_entity_root_tx.index.isin(df_filtered.index)

    df_entity_root_tx['original_value_usd'] = entity_root_historicalUSD

    df_entity_root_tx['pct_total_value'] = df_entity_root_tx['historicalUSD'] / df_entity_root_tx['original_value_usd']

    df_in_tx_counts = df_entity_root_tx.groupby(["fromAddress.address","tx_direction"])['id'].count().reset_index().rename(columns={'id':'no_of_in_interactions'})
    df_in_tx_counts = df_in_tx_counts[df_in_tx_counts['tx_direction'] == 'IN'][['fromAddress.address','no_of_in_interactions']]
    df_entity_root_tx = pd.merge(df_entity_root_tx,df_in_tx_counts, on='fromAddress.address',how='left')
    df_entity_root_tx['no_of_in_interactions'].fillna(0, inplace=True)

    df_out_tx_counts = df_entity_root_tx.groupby(["toAddress.address","tx_direction"])['id'].count().reset_index().rename(columns={'id':'no_of_out_interactions'})
    df_out_tx_counts = df_out_tx_counts[df_out_tx_counts['tx_direction'] == 'OUT'][['toAddress.address','no_of_out_interactions']]
    df_entity_root_tx = pd.merge(df_entity_root_tx,df_out_tx_counts, on='toAddress.address',how='left')
    df_entity_root_tx['no_of_out_interactions'].fillna(0, inplace=True)

    ####Define and set the tx_to_follow conditions####
    to_follow_cond1 = df_entity_root_tx['first_in_tx'] == True
    to_follow_cond2a = df_entity_root_tx['pct_total_value'] > pct_total_value_threshold
    to_follow_cond2b = df_entity_root_tx['tx_direction'] == 'OUT'

    #add the block below because due to data quality issues form the API the original_tx cannot be found
    if df_entity_root_tx['original_tx'].any():
        original_tx_timestamp = df_entity_root_tx[df_entity_root_tx['original_tx']]['blockTimestamp'].min()
        to_follow_cond2c = df_entity_root_tx['blockTimestamp'] > original_tx_timestamp 
    else:
        logger.info(f"No original transaction found for address {entity_root}")
        to_follow_cond2c = pd.Series([True] * len(df_entity_root_tx))  # Create a condition that is always True, so it doesn't filter anything

    to_follow_cond3a = df_entity_root_tx['no_of_in_interactions'] >= no_of_in_interactions_threshold
    to_follow_cond3b = df_entity_root_tx['no_of_out_interactions'] >= no_of_out_interactions_threshold 
    
    to_follow_cond4 = df_entity_root_tx['historicalUSD'] > USD_tx_threshold
    to_follow_cond4b = df_entity_root_tx['tx_direction'] == 'OUT'

    df_entity_root_tx['tx_to_follow'] = np.where((to_follow_cond1) | (to_follow_cond2a & to_follow_cond2b & to_follow_cond2c) | (to_follow_cond3a & to_follow_cond3b) | (to_follow_cond4 & to_follow_cond4b),True,False) 

    # Get the transactionHashes that led to tx_to_follow being True
    df_tx_to_follow = df_entity_root_tx[df_entity_root_tx['tx_to_follow']]


    # Return as a list of tuples (address, transactionHash)
    tx_to_follow_tuples = list(zip(df_tx_to_follow['fromAddress.address'], df_tx_to_follow['transactionHash'], [entity_name]*len(df_tx_to_follow)))
    tx_to_follow_tuples.extend(list(zip(df_tx_to_follow['toAddress.address'], df_tx_to_follow['transactionHash'], [entity_name]*len(df_tx_to_follow))))

    return tx_to_follow_tuples, exchange_deposit_addresses, df_all_transactions

def address_identification(entity_name, entity_root,initial_transaction_hash,entity_root_historicalUSD,entities_to_include, filter_from_date):

    #the dataframe where all the transactions are stored for the processed addresses
    df_all_transactions = pd.DataFrame()

    # List for storing all processed addresses
    identified_addresses = [(entity_root, initial_transaction_hash, entity_name, "initialization_identification_method")]

    #add manual addresses here (and add the entity transfers here)

    #keep track of the processed addresses to prevent double processing
    processed_addresses = set()  # a set of processed addresses

    # Stack for processing addresses, contains pairs of address and associated transaction hash
    stack_addresses = [(entity_root, initial_transaction_hash, entity_name)]
    all_exchange_deposit_addresses = set() 

    while stack_addresses:
        current_address, transaction_hash, current_entity_name = stack_addresses.pop()  # get (and remove) the address at the end of the stack

        # Skip if the address has already been processed
        if current_address in processed_addresses:
            continue

        logger.info(f"Processing address: {current_address}")
        processed_addresses.add(current_address)  # add the address to the set of processed addresses
        found_tuples, exchange_deposit_addresses, df_all_transactions  = process_addresses(current_entity_name, current_address, transaction_hash,entity_root_historicalUSD,entities_to_include, df_all_transactions, filter_from_date)   # process this address
        all_exchange_deposit_addresses.update(exchange_deposit_addresses)  # store all exchange deposit addresses
        found_tuples = list(set(found_tuples)) # Remove duplicates from found_tuples
        
        # Add the new unique addresses to the stack_addresses list
        new_tuples = [tuple for tuple in found_tuples if tuple[0] not in processed_addresses and tuple[0] not in (addr for addr, _, _, _ in identified_addresses)]
        stack_addresses.extend(new_tuples)

        # Add the new unique addresses to the identified_addresses list
        for tuple in new_tuples:
            if tuple[0] not in (addr for addr, _, _, _ in identified_addresses):
                identified_tuple = (tuple[0], tuple[1], tuple[2], "general_identification_method")
                identified_addresses.append(identified_tuple)

        # Save state after processing each address and updating lists
        logger.info(f"Saving intermediate state after processing address: {current_address}")
        save_state(df_all_transactions, identified_addresses)

    logger.info("\n")
    logger.info("General identification process finished")
    logger.info("Start exchange deposit identification")
    logger.info("\n")

    # Remove duplicates from all_exchange_deposit_addresses
    all_exchange_deposit_addresses = list(set(all_exchange_deposit_addresses))
    logger.info("Number of exchange deposit addresses to be processed: %s", len(set(all_exchange_deposit_addresses)))

    ###### Process exchange deposit addresses
    logger.info("\n")
    logger.info("Get all incoming user addresses for those addresses..")
    #get the previous senders to the identified exchange deposit addresses
    previous_senders = process_exchange_deposit(all_exchange_deposit_addresses, filter_from_date)  

    logger.info("\n")
    logger.info("Number of identified addresses by the exchange deposit method: %s", sum(list(set( (len(v) for v in previous_senders.values())))))
    logger.info("Get all transactions for those addresses..")
    logger.info("\n")
    #get all transactions for the previous senders
    new_transactions = get_extra_transactions(entity_name, previous_senders, filter_from_date)
    logger.info("\n")
    logger.info("the shape of new_transactions is %s", new_transactions.shape)
    #concat the main df with the new df and drop duplicates 
    combined_df = pd.concat([df_all_transactions, new_transactions])
    logger.info("the shape of combined_df is %s", combined_df.shape)
    combined_df.drop_duplicates(subset=['id', 'tx_direction'], keep='first', inplace=True)
    #store as main df
    df_all_transactions = combined_df
    logger.info("the shape of df_all_transactions is %s", df_all_transactions.shape)

    for address, tx_hashes in previous_senders.items():
        if isinstance(tx_hashes, dict):  # If tx_hashes is a dictionary
            for nested_address, nested_tx_hash in tx_hashes.items(): 
                if nested_address not in (addr for addr, _, _, _ in identified_addresses): 
                    identified_tuple = (nested_address, nested_tx_hash, entity_name, "exchange_deposit_identification_method")
                    identified_addresses.append(identified_tuple) 
        else: # If tx_hashes is not a dictionary, it should be a single transaction hash string.
            if address not in (addr for addr, _, _, _ in identified_addresses): 
                identified_tuple = (address, tx_hashes, entity_name, "exchange_deposit_identification_method")
                identified_addresses.append(identified_tuple)

    logger.info("\n")
    logger.info("All identified addresses: %s", [addr for addr, _, _, _ in identified_addresses])
    return df_all_transactions, identified_addresses



if __name__ == "__main__":
    for entity in entities:
        entity_name = entity['entity_name']
        arkham_entity = entity['arkham_entity']
        filter_from_date = entity['filter_from_date']
        entities_to_include = entity['entities_to_include']

    ################add those for single entity input##################################
        if single_address_input:
            entity_root = entity['base_contract_address']
            entity_tx_hash = entity['entity_tx_hash']
            entity_root_historicalUSD = entity['historicalUSD']
        else:
            base_contract_address = entity['base_contract_address']
            root_filters = entity['root_filters']
            # Get the transfers
            logger.info(f"Getting transfers for entity: {entity_name}")
            transfers = get_transfers(base_contract_address)

            #Extract entity root
            logger.info(f"Extracting entity root for entity: {entity_name}")
            entity_root, entity_tx_hash, entity_root_historicalUSD = extract_entity_root(transfers, root_filters) 
    ####################################################################################

        logger.info(f"Processing entity: {entity_name}")

        # Process the addresses
        logger.info(f"Processing addresses for entity: {entity_name}")
        df_all_transactions, identified_addresses = address_identification(entity_name, entity_root, entity_tx_hash, entity_root_historicalUSD, entities_to_include, filter_from_date) 
        print(identified_addresses)