import requests
import pandas as pd
import requests
import time
import numpy as np
from datetime import datetime
from dateutil.parser import parse
import json
import os
from variables import entities
from supporting_functions import add_helper_columns, get_extra_transactions, find_entity_by_name, convert_date_to_unix_milliseconds, process_signalised_address, get_protocol_balances
from settings import get_config, headers, url_transfers, sleep_time, usd_threshold, limit, CALL_SWARM, SWARM_EVALUATE_URL
from signal_functions import *
from messaging_functions import *
import logging
logger = logging.getLogger(__name__)

config_values = get_config()
api_key = config_values['api_key']
bot_token = config_values['bot_token'] 
chat_ID = config_values['chat_ID']



def trigger_swarm_evaluation(transaction_hash, signal_type, signal_value, signal_row_for_s5, entities_for_s5):
    """
    Constructs the reason and calls the Swarm evaluation endpoint.
    """
    if not CALL_SWARM:
        return

    try:
        # Get the human-readable signal message using the same logic as construct_alert_message
        transform_func = signal_value_transforms.get(signal_type, lambda x: str(x))
        if signal_type in ['S5a Daily Cumulative Volume Exceeded', 'S5b Daily Absolute Volume Exceeded']:
            # For S5 signals, we need to pass the signal_row and entities to the transform function
            # We need to ensure signal_row_for_s5 and entities_for_s5 are correctly passed or constructed here
            # For now, assuming signal_row_for_s5 and entities_for_s5 are passed correctly
            reason_message = transform_func(signal_value, signal_row_for_s5, entities_for_s5)
        else:
            reason_message = transform_func(signal_value)
        
        reason = f"{signal_type}: {reason_message}"

        params = {
            'transaction_hash': transaction_hash,
            'reason': reason
        }
        response = requests.get(SWARM_EVALUATE_URL, params=params)
        response.raise_for_status() # Raise an exception for HTTP errors
        logger.info(f"Successfully triggered Swarm evaluation for {transaction_hash}. Response: {response.json()}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling Swarm evaluation for {transaction_hash}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in trigger_swarm_evaluation for {transaction_hash}: {e}")



def initialize_monitoring(df_all_transactions):

        # Create an empty dataframe to store signals
    df_signals = pd.DataFrame(columns=['Timestamp','transactionHash','entity_name', 'Signal_Value','Signal_Type', 'root_address','interacting_address','tx_direction','to_name_label', 'from_name_label','unitValue','tokenSymbol','historicalUSD','chain'])

    # Create a dictionary to store the latest seen timestamp for each address
    #last_seen_tx_timestamps = {addr: None for addr in df_all_transactions['root_address'].unique()}

    # Get the current timestamp in Unix millisecond format.
    current_timestamp = int(datetime.now().timestamp() * 1000)
    last_seen_tx_timestamps = {addr: current_timestamp for addr in df_all_transactions['root_address'].unique()}
    

    #adding protocol balances to the most recent transaction per address
    df_all_transactions['protocol_balances'] = None
    unique_addresses = df_all_transactions['root_address'].unique()
    unique_addresses = unique_addresses[pd.notnull(unique_addresses)]


    logger.info(f"call to get {len(unique_addresses)} protocol balances..")

    for address in unique_addresses:
        try:
            logger.info("Trying to Fetch protocol balances...")
            balances = get_protocol_balances(address)
            most_recent_tx_row = df_all_transactions[df_all_transactions['root_address'] == address]['blockTimestamp'].idxmax()
            df_all_transactions.at[most_recent_tx_row, 'protocol_balances'] = str(balances)

        except Exception as e:
            logger.info(f"Error getting balances for address {address}: {e}")
    count_protocol_balances = df_all_transactions[(df_all_transactions['protocol_balances'] != '{}') & (df_all_transactions['protocol_balances'].notna())].shape[0]
    logger.info(f"found {count_protocol_balances} protocol balances")

    #logger.info("Initial state:")
    #logger.info("last_seen_tx_timestamps: %s", last_seen_tx_timestamps)

    # Store the current day when starting the program
    current_day = datetime.now().day
    today_signal_5a_addresses = set()
    today_signal_5b_addresses = set()
    today_signal_6a_addresses = set()
    today_signal_6b_addresses = set()

    #limit = 1000
    offset = 0


    return df_signals, last_seen_tx_timestamps, current_day, today_signal_5a_addresses, today_signal_5b_addresses, today_signal_6a_addresses, today_signal_6b_addresses, offset


def monitor_transactions(df_all_transactions, identified_addresses, df_signals, last_seen_tx_timestamps, current_day, today_signal_5a_addresses, today_signal_5b_addresses, today_signal_6a_addresses, today_signal_6b_addresses,offset):

    # only get the identified_addresses
    addresses_only = [pair[0] for pair in identified_addresses if pair[3] != 'removed by user']

    #filter out the ones that may be removed by the user
    last_seen_tx_timestamps = {k: v for k, v in last_seen_tx_timestamps.items() if k in addresses_only}

    #only get the entity_names
    entity_names = [pair[2] for pair in identified_addresses]

    # Convert that list to a numpy array
    all_addresses = np.array(addresses_only, dtype='object') #df_all_transactions['root_address'].dropna().unique()  maybe change back to

    logger.info(f"\nChecking for new transactions for {len(all_addresses)} addresses...")

    # Loop through all unique addresses
    for idx, address in enumerate(all_addresses):

        #fetch the current entity_name
        current_entity_name = entity_names[idx]
        the_entity = find_entity_by_name(entities, current_entity_name)
        filter_from_date = the_entity['filter_from_date']

        # Fetch the last seen transaction timestamp for this address
        last_seen_timestamp = last_seen_tx_timestamps.get(address)

        # If a timestamp exists for this address, convert it to Unix millisecond timestamp
        if last_seen_timestamp is not None:
            if isinstance(last_seen_timestamp, int) or (isinstance(last_seen_timestamp, str) and last_seen_timestamp.isdigit()):
                # Unix millisecond timestamp format
                timeGte = int(last_seen_timestamp) + 10
            else:
                # Datetime format
                dt_obj = pd.to_datetime(last_seen_timestamp)
                dt_obj += pd.Timedelta(milliseconds=10)
                
                # Convert datetime object to Unix millisecond timestamp
                timeGte = int(dt_obj.timestamp() * 1000)
        else:
            timeGte = None
            
        # Call the API, passing in the last seen transaction timestamp for this address as a parameter
        params = {
            'base': address,
            'limit': limit,
            'offset': offset
        }
    
        # If a timestamp exists for this address, include it in the params
        if timeGte is not None:
            params['timeGte'] = timeGte

        #logger.info(f"API call parameters for address {address}: {params}")

        try:
            response = requests.get(url_transfers, params=params, headers=headers)     
            # Check status code
            if response.status_code == 200:
                if not response.content:
                    logger.info(f"Successful request to '{url_transfers}' with parameters {params}, but received an empty response.")
                    df_new_txs = pd.DataFrame()  # Create an empty dataframe as there is no content
                else:
                    df_new_txs = pd.json_normalize(response.json().get('transfers', []))
            else:
                logger.info(f"Error with status code {response.status_code} when accessing '{url_transfers}' with parameters {params}. Response: {response.text}")



        except requests.exceptions.RequestException as e:
            logger.info(f"Request error: {e}")
            df_new_txs = pd.DataFrame()

        except json.JSONDecodeError as e:
            logger.info(f"JSON decoding error: {e}")
            df_new_txs = pd.DataFrame()

        # If there are any new transactions, update df_all_transactions and the timestamp for this address
        if not df_new_txs.empty:
              
            # Filter out the transactions that already exist in df_all_transactions
            df_new_txs = df_new_txs[~df_new_txs['transactionHash'].isin(df_all_transactions['transactionHash'])]

            #logger.info(f"Timestamps of transactions after filtering for address {address}: {df_new_txs['blockTimestamp'].to_list()}")

            # If there are still any new transactions after filtering, append them to df_all_transactions and update the timestamp
            if not df_new_txs.empty:
                logger.info("The number of new transactions is: %s", df_new_txs.shape[0])

                #add helper columns
                # Initialize an empty DataFrame to hold the result
                df_result = pd.DataFrame()

                # Loop through each row in df_new_txs
                for index, row in df_new_txs.iterrows():
                    # Convert the row into a DataFrame
                    df_row = pd.DataFrame(row).transpose()
                    # Apply add_helper_columns on the row
                    df_row = add_helper_columns(current_entity_name, df_row, address,get_protocol_balance=True)
                    # Append the result to df_result
                    df_result = pd.concat([df_result, df_row], axis=0)

                # Reset the index of df_result
                df_result.reset_index(drop=True, inplace=True)
                df_new_txs = df_result.copy()
                logger.info(df_new_txs.to_string())
            

                df_all_transactions = pd.concat([df_all_transactions, df_new_txs], axis=0, ignore_index=True, sort=False)
                last_seen_tx_timestamps[address] = convert_date_to_unix_milliseconds(df_new_txs['blockTimestamp'].max())

                #Create an empty list to store the pairs of signalised identified addresses in every address loop
                signal_identified_pairs = set() 

                logger.info(f"The number of new transactions for address {address} found is {df_new_txs.shape[0]}")
                

############################Start the signaling process by processing one new transaction at a time #####################################################
                for index, row in df_new_txs.iterrows():
                        # Check if the day has changed
                    if datetime.now().day != current_day:
                        # If the day has changed, clear the set and update the current day
                        today_signal_5a_addresses.clear()
                        today_signal_5b_addresses.clear()
                        today_signal_6a_addresses.clear()
                        today_signal_6b_addresses.clear()
                        current_day = datetime.now().day

                    logger.info("The timestamp for the transaction that is being processed: %s", row['blockTimestamp'])

#BEGIN######################get signal from S1_fresh_wallet and append it to df_signals##################################################################
                    #add a threshold of 50 usd to be considered
                    if row['historicalUSD'] > usd_threshold:

                        signal_S1 = S1_fresh_wallet(row['interacting_address'])
                        # Check if total usd balance is 0 or less 
                        if (signal_S1[0] | signal_S1[2]) & signal_S1[3]:
                            logger.info(f"Conditions met for S1_fresh_wallet for transaction {row['transactionHash']}.")
                            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                            'transactionHash': [row['transactionHash']],
                            'entity_name': [row['entity_name']],
                            'Signal_Value': [signal_S1],
                            'Signal_Type': ['S1 Fresh Wallet'],
                            'root_address': [row['root_address']],
                            'interacting_address': [row['interacting_address']],
                            'tx_direction': [row['tx_direction']],
                            'to_name_label': [row['to_name_label']],
                            'from_name_label': [row['from_name_label']],
                            'unitValue': [row['unitValue']],
                            'tokenSymbol': [row['tokenSymbol']],
                            'historicalUSD': [row['historicalUSD']],
                            'chain': [row['chain']]})
                            df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                            # Print the new signal that was added to df_signals
                            logger.info(f"Added S1_fresh_wallet signal for transaction {row['transactionHash']}: {signal_S1}")

                            #send the message
                            try:
                                # Send the message
                                logging.info('Constructing alert message...')
                                message = construct_alert_message(new_row.iloc[0])
                                
                                # Log the constructed message for debugging
                                logging.info(f'Constructed Message: {message}')
                                
                                logging.info('Sending telegram message...')
                                send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                logging.info('Telegram message sent successfully.')
                                
                            except Exception as e:
                                logging.exception('An error occurred while sending telegram message:')

                            # Before adding anything to signal_identified_pairs and last_seen_tx_timestamps,
                            # Check if the 'toAddress.arkhamEntity.name' and 'toAddress.arkhamEntity.type' are empty if tx_direction equals 'OUT'
                            # and 'fromAddress.arkhamEntity.name', 'fromAddress.arkhamEntity.type' are empty if tx_direction equals 'IN'
                            process_signalised_address(row, signal_identified_pairs, identified_addresses, last_seen_tx_timestamps)

                            trigger_swarm_evaluation(row['transactionHash'], 'S1 Fresh Wallet', signal_S1, row, entities)

#END########################get signal from S1_fresh_wallet and append it to df_signals##################################################################

#BEGIN######################get signals from S2_interaction_new_protocol and append them to df_signals###################################################
                    logger.info("calling the signal_S2 function")
                    signal_S2 = S2_interaction_new_protocol(df_all_transactions, row)
                    logger.info("Done with calling the signal_S2 function. The value of signal_s2: %s", signal_S2)

                    # If the conditions for S2 are met, append it to df_signals
                    if signal_S2[0] == True and signal_S2[2] == False:
                        logger.info(f"Conditions met for S2_interaction_new_protocol for transaction {row['transactionHash']}.")
                        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                        'transactionHash': [row['transactionHash']],
                        'entity_name': [row['entity_name']],
                        'Signal_Value': [signal_S2],
                        'Signal_Type': ['S2 Interaction New Protocol'],
                        'root_address': [row['root_address']],
                        'interacting_address': [row['interacting_address']],
                        'tx_direction': [row['tx_direction']], 
                        'to_name_label': [row['to_name_label']],  
                        'from_name_label': [row['from_name_label']],
                        'unitValue': [row['unitValue']],
                        'tokenSymbol': [row['tokenSymbol']],
                        'historicalUSD': [row['historicalUSD']],
                        'chain': [row['chain']]})
                        df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                        # Print the new signal that was added to df_signals
                        logger.info(f"Added S2_interaction_new_protocol signal for transaction {row['transactionHash']}: {signal_S2}")

                        #send the message
                        try:
                            # Send the message
                            logging.info('Constructing alert message...')
                            message = construct_alert_message(new_row.iloc[0])
                            
                            # Log the constructed message for debugging
                            logging.info(f'Constructed Message: {message}')
                            
                            logging.info('Sending telegram message...')
                            send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                            logging.info('Telegram message sent successfully.')
                            
                        except Exception as e:
                            logging.exception('An error occurred while sending telegram message:')

                        trigger_swarm_evaluation(row['transactionHash'], 'S2 Interaction New Protocol', signal_S2, row, entities)

#END######################get signals from S2_interaction_new_protocol and append them to df_signals###################################################

#BEGIN######################get signals from S3_interaction_new_crypto and append them to df_signals###################################################
                    if row['historicalUSD'] > usd_threshold:

                        signal_S3 = S3_interaction_new_crypto(df_all_transactions, row)
                        if signal_S3[0] == True:
                            logger.info(f"Conditions met for S3_interaction_new_crypto for transaction {row['transactionHash']}.")
                            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                            'transactionHash': [row['transactionHash']],
                            'entity_name': [row['entity_name']],
                            'Signal_Value': [signal_S3],
                            'Signal_Type': ['S3 Interaction New Crypto'],
                            'root_address': [row['root_address']],
                            'interacting_address': [row['interacting_address']],
                            'tx_direction': [row['tx_direction']], 
                            'to_name_label': [row['to_name_label']],  
                            'from_name_label': [row['from_name_label']],
                            'unitValue': [row['unitValue']],
                            'tokenSymbol': [row['tokenSymbol']],
                            'historicalUSD': [row['historicalUSD']],
                            'chain': [row['chain']]})

                            df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                            # Print the new signal that was added to df_signals
                            logger.info(f"Added S3_interaction_new_crypto signal for transaction {row['transactionHash']}: {signal_S3}")

                            #send the message
                            try:
                                # Send the message
                                logging.info('Constructing alert message...')
                                message = construct_alert_message(new_row.iloc[0])
                                
                                # Log the constructed message for debugging
                                logging.info(f'Constructed Message: {message}')
                                
                                logging.info('Sending telegram message...')
                                send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                logging.info('Telegram message sent successfully.')
                                
                            except Exception as e:
                                logging.exception('An error occurred while sending telegram message:')

                            trigger_swarm_evaluation(row['transactionHash'], 'S3 Interaction New Crypto', signal_S3, row, entities)

#END######################get signals from S3_interaction_new_crypto and append them to df_signals###################################################

#BEGIN######################get signals from S4_interaction_new_exchange and append them to df_signals###################################################
                    if row['historicalUSD'] > usd_threshold:
                        signal_S4 = S4_interaction_new_exchange(df_all_transactions, row)
                        if signal_S4[0] == True:
                            logger.info(f"Conditions met for S4_interaction_new_exchange for transaction {row['transactionHash']}.")
                            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                            'transactionHash': [row['transactionHash']],
                            'entity_name': [row['entity_name']],
                            'Signal_Value': [signal_S4],
                            'Signal_Type': ['S4 Interaction New Exchange'],
                            'root_address': [row['root_address']],
                            'interacting_address': [row['interacting_address']],
                            'tx_direction': [row['tx_direction']], 
                            'to_name_label': [row['to_name_label']],  
                            'from_name_label': [row['from_name_label']],
                            'unitValue': [row['unitValue']],
                            'tokenSymbol': [row['tokenSymbol']],
                            'historicalUSD': [row['historicalUSD']],
                            'chain': [row['chain']]})
                            df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                            # Print the new signal that was added to df_signals
                            logger.info(f"Added S4_interaction_new_exchange signal for transaction {row['transactionHash']}: {signal_S4}")

                            #send the message
                            try:
                                # Send the message
                                logging.info('Constructing alert message...')
                                message = construct_alert_message(new_row.iloc[0])
                                
                                # Log the constructed message for debugging
                                logging.info(f'Constructed Message: {message}')
                                
                                logging.info('Sending telegram message...')
                                send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                logging.info('Telegram message sent successfully.')
                                
                            except Exception as e:
                                logging.exception('An error occurred while sending telegram message:')

                            trigger_swarm_evaluation(row['transactionHash'], 'S4 Interaction New Exchange', signal_S4, row, entities)
                    
#END######################get signals from S4_interaction_new_exchange and append them to df_signals###################################################

#BEGIN######################get signals from S5a_daily_volume_alert_cum and append them to df_signals###################################################
                    # If the address has not generated a signal today, check for the S5a and S5b signals
             
                    if row['root_address'] not in today_signal_5a_addresses:
                        if row['historicalUSD'] > usd_threshold:
                            # Get the token_ids from the dataframe
                            token_id = [row['tokenId']]
                            # Create the exchange rate dictionary
                            exchange_rate_usd_dict = create_exchange_rate_usd_dict(token_id)
                            # Call your function
                            signal_S5a = S5a_daily_volume_alert_cum(df_all_transactions, row, exchange_rate_usd_dict, the_entity)
                            if signal_S5a[0] == True:
                                logger.info(f"Conditions met for S5a_daily_volume_alert_cum for transaction {row['transactionHash']}.")
                                current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                                'transactionHash': [row['transactionHash']],
                                'entity_name': [row['entity_name']],
                                'Signal_Value': [signal_S5a],
                                'Signal_Type': ['S5a Daily Cumulative Volume Exceeded'],
                                'root_address': [row['root_address']],
                                'interacting_address': [row['interacting_address']],
                                'tx_direction': [row['tx_direction']], 
                                'to_name_label': [row['to_name_label']],  
                                'from_name_label': [row['from_name_label']],
                                'unitValue': [row['unitValue']],
                                'tokenSymbol': [row['tokenSymbol']],
                                'historicalUSD': [row['historicalUSD']],
                                'chain': [row['chain']]})
                                df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                                # Add root address to the set of addresses that have generated a signal today
                                today_signal_5a_addresses.add(row['root_address'])

                                # Print the new signal that was added to df_signals
                                logger.info(f"Added S5a_daily_volume_alert_cum signal for transaction {row['transactionHash']}: {signal_S5a}")

                                #send the message
                                try:
                                    # Send the message
                                    logging.info('Constructing alert message...')
                                    message = construct_alert_message(new_row.iloc[0])
                                    
                                    # Log the constructed message for debugging
                                    logging.info(f'Constructed Message: {message}')
                                    
                                    logging.info('Sending telegram message...')
                                    send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                    logging.info('Telegram message sent successfully.')
                                    
                                except Exception as e:
                                    logging.exception('An error occurred while sending telegram message:')

                                trigger_swarm_evaluation(row['transactionHash'], 'S5a Daily Cumulative Volume Exceeded', signal_S5a, row, entities)

    #END######################get signals from S5a_daily_volume_alert_cum and append them to df_signals###################################################


    #BEGIN######################get signals from S5b_daily_volume_alert_abs and append them to df_signals###################################################
                    # If the address has not generated a signal today, check for the S5a and S5b signals
                    if row['root_address'] not in today_signal_5b_addresses:
                        if row['historicalUSD'] > usd_threshold:
                            # Call your function
                            signal_S5b = S5b_daily_volume_alert_abs(row, the_entity)
                            if signal_S5b[0] == True:
                                logger.info(f"Conditions met for S5b_daily_volume_alert_abs for transaction {row['transactionHash']}.")
                                current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                                'transactionHash': [row['transactionHash']],
                                'entity_name': [row['entity_name']],
                                'Signal_Value': [signal_S5b],
                                'Signal_Type': ['S5b Daily Absolute Volume Exceeded'],
                                'root_address': [row['root_address']],
                                'interacting_address': [row['interacting_address']],
                                'tx_direction': [row['tx_direction']], 
                                'to_name_label': [row['to_name_label']],  
                                'from_name_label': [row['from_name_label']],
                                'unitValue': [row['unitValue']],
                                'tokenSymbol': [row['tokenSymbol']],
                                'historicalUSD': [row['historicalUSD']],
                                'chain': [row['chain']]})
                                df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                                # Add root address to the set of addresses that have generated a signal today
                                today_signal_5b_addresses.add(row['root_address'])

                                # Print the new signal that was added to df_signals
                                logger.info(f"Added S5b_daily_volume_alert_abs signal for transaction {row['transactionHash']}: {signal_S5b}")

                                #send the message
                                try:
                                    # Send the message
                                    logging.info('Constructing alert message...')
                                    message = construct_alert_message(new_row.iloc[0])
                                    
                                    # Log the constructed message for debugging
                                    logging.info(f'Constructed Message: {message}')
                                    
                                    logging.info('Sending telegram message...')
                                    send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                    logging.info('Telegram message sent successfully.')
                                    
                                except Exception as e:
                                    logging.exception('An error occurred while sending telegram message:')

                                trigger_swarm_evaluation(row['transactionHash'], 'S5b Daily Absolute Volume Exceeded', signal_S5b, row, entities)

                                # Before adding anything to signal_identified_pairs and last_seen_tx_timestamps,
                                # Check if the 'toAddress.arkhamEntity.name' and 'toAddress.arkhamEntity.type' are empty if tx_direction equals 'OUT'
                                # and 'fromAddress.arkhamEntity.name', 'fromAddress.arkhamEntity.type' are empty if tx_direction equals 'IN'
                                process_signalised_address(row, signal_identified_pairs, identified_addresses, last_seen_tx_timestamps)

    #END######################get signals from S5b_daily_volume_alert_abs and append them to df_signals###################################################

    #BEGIN######################get signals from S6a_d_freq_change and append them to df_signals###################################################
                    if row['root_address'] not in today_signal_6a_addresses:
                        if row['historicalUSD'] > usd_threshold:
                            # Call your function
                            signal_S6a = S6a_d_freq_change(df_all_transactions, row['root_address'], row['interacting_address'])
                            if signal_S6a[0] == True:
                                logger.info(f"Conditions met for S6a_d_freq_change for transaction {row['transactionHash']}.")
                                current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                                'transactionHash': [row['transactionHash']],
                                'entity_name': [row['entity_name']],
                                'Signal_Value': [signal_S6a],
                                'Signal_Type': ['S6a Daily Frequency Change'],
                                'root_address': [row['root_address']],
                                'interacting_address': [row['interacting_address']],
                                'tx_direction': [row['tx_direction']], 
                                'to_name_label': [row['to_name_label']],  
                                'from_name_label': [row['from_name_label']],
                                'unitValue': [row['unitValue']],
                                'tokenSymbol': [row['tokenSymbol']],
                                'historicalUSD': [row['historicalUSD']],
                                'chain': [row['chain']]})
                                df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                                # Add interacting address to the set of addresses that have generated a signal today
                                today_signal_6a_addresses.add(row['root_address'])

                                # Print the new signal that was added to df_signals
                                logger.info(f"Added S6a_d_freq_change signal for transaction {row['transactionHash']}: {signal_S6a}")

                                #send the message
                                try:
                                    # Send the message
                                    logging.info('Constructing alert message...')
                                    message = construct_alert_message(new_row.iloc[0])
                                    
                                    # Log the constructed message for debugging
                                    logging.info(f'Constructed Message: {message}')
                                    
                                    logging.info('Sending telegram message...')
                                    send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                    logging.info('Telegram message sent successfully.')
                                    
                                except Exception as e:
                                    logging.exception('An error occurred while sending telegram message:')

                                trigger_swarm_evaluation(row['transactionHash'], 'S6a Daily Frequency Change', signal_S6a, row, entities)
                        
#END######################get signals from S6a_d_freq_change and append them to df_signals###################################################

#BEGIN######################get signals from S6b_w_freq_change and append them to df_signals###################################################
                    if row['root_address'] not in today_signal_6b_addresses:
                        if row['historicalUSD'] > usd_threshold:
                            # Call your function
                            signal_S6b = S6b_w_freq_change(df_all_transactions, row['root_address'], row['interacting_address'])
                            if signal_S6b[0] == True:
                                logger.info(f"Conditions met for S6b_w_freq_change for transaction {row['transactionHash']}.")
                                current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                                new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                                'transactionHash': [row['transactionHash']],
                                'entity_name': [row['entity_name']],
                                'Signal_Value': [signal_S6b],
                                'Signal_Type': ['S6b Weekly Frequency Change'],
                                'root_address': [row['root_address']],
                                'interacting_address': [row['interacting_address']],
                                'tx_direction': [row['tx_direction']], 
                                'to_name_label': [row['to_name_label']],  
                                'from_name_label': [row['from_name_label']],
                                'unitValue': [row['unitValue']],
                                'tokenSymbol': [row['tokenSymbol']],
                                'historicalUSD': [row['historicalUSD']],
                                'chain': [row['chain']]})
                                df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                                # Add interacting address to the set of addresses that have generated a signal today
                                today_signal_6b_addresses.add(row['root_address'])

                                # Print the new signal that was added to df_signals
                                logger.info(f"Added S6b_w_freq_change signal for transaction {row['transactionHash']}: {signal_S6b}")

                                #send the message
                                try:
                                    # Send the message
                                    logging.info('Constructing alert message...')
                                    message = construct_alert_message(new_row.iloc[0])
                                    
                                    # Log the constructed message for debugging
                                    logging.info(f'Constructed Message: {message}')
                                    
                                    logging.info('Sending telegram message...')
                                    send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                    logging.info('Telegram message sent successfully.')
                                    
                                except Exception as e:
                                    logging.exception('An error occurred while sending telegram message:')

                                trigger_swarm_evaluation(row['transactionHash'], 'S6b Weekly Frequency Change', signal_S6b, row, entities)
            
#END######################get signals from S6b_w_freq_change and append them to df_signals###################################################

#BEGIN######################get signals from S7_LP_token_traded and append them to df_signals###################################################
                    if os.environ.get("ACTIVE_PROTOCOL_SIGNAL"):
                        # Call your function
                        signal_S7 = S7_protocol_activity(row,df_all_transactions)
                        if signal_S7[0] == True:
                            logger.info(f"Conditions met for S7_protocol_activity for transaction {row['transactionHash']}.")
                            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                            'transactionHash': [row['transactionHash']],
                            'entity_name': [row['entity_name']],
                            'Signal_Value': [signal_S7],
                            'Signal_Type': ['S7 protocol activity'],
                            'root_address': [row['root_address']],
                            'interacting_address': [row['interacting_address']],
                            'tx_direction': [row['tx_direction']], 
                            'to_name_label': [row['to_name_label']],  
                            'from_name_label': [row['from_name_label']],
                            'unitValue': [row['unitValue']],
                            'tokenSymbol': [row['tokenSymbol']],
                            'historicalUSD': [row['historicalUSD']],
                            'chain': [row['chain']]})
                            df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                            # Print the new signal that was added to df_signals
                            logger.info(f"Added S7_protocol_activity signal for transaction {row['transactionHash']}: {signal_S7}")

                            #send the message
                            try:
                                # Send the message
                                logging.info('Constructing alert message...')
                                message = construct_alert_message(new_row.iloc[0])
                                
                                # Log the constructed message for debugging
                                logging.info(f'Constructed Message: {message}')
                                
                                logging.info('Sending telegram message...')
                                send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                                logging.info('Telegram message sent successfully.')
                                
                            except Exception as e:
                                logging.exception('An error occurred while sending telegram message:')

                            trigger_swarm_evaluation(row['transactionHash'], 'S7 protocol activity', signal_S7, row, entities)
#END######################get signals from S7_LP_token_traded and append them to df_signals###################################################


#BEGIN######################get signals from S8_LP_token_traded and append them to df_signals###################################################
                    # Call your function
                    signal_S8 = S8_LP_token_traded(row)
                    if signal_S8[0] == True:
                        logger.info(f"Conditions met for S8_LP_token_traded for transaction {row['transactionHash']}.")
                        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        new_row = pd.DataFrame({'Timestamp': [current_timestamp],
                        'transactionHash': [row['transactionHash']],
                        'entity_name': [row['entity_name']],
                        'Signal_Value': [signal_S8],
                        'Signal_Type': ['S8 LP token traded'],
                        'root_address': [row['root_address']],
                        'interacting_address': [row['interacting_address']],
                        'tx_direction': [row['tx_direction']], 
                        'to_name_label': [row['to_name_label']],  
                        'from_name_label': [row['from_name_label']],
                        'unitValue': [row['unitValue']],
                        'tokenSymbol': [row['tokenSymbol']],
                        'historicalUSD': [row['historicalUSD']],
                        'chain': [row['chain']]})
                        df_signals = pd.concat([df_signals, new_row], ignore_index=True)

                        # Print the new signal that was added to df_signals
                        logger.info(f"Added S8_LP_token_traded signal for transaction {row['transactionHash']}: {signal_S8}")

                        #send the message
                        try:
                            # Send the message
                            logging.info('Constructing alert message...')
                            message = construct_alert_message(new_row.iloc[0])
                            
                            # Log the constructed message for debugging
                            logging.info(f'Constructed Message: {message}')
                            
                            logging.info('Sending telegram message...')
                            send_telegram_message(bot_token, chat_ID, message, row['root_address'])
                            logging.info('Telegram message sent successfully.')
                            
                        except Exception as e:
                            logging.exception('An error occurred while sending telegram message:')

                        trigger_swarm_evaluation(row['transactionHash'], 'S8 LP token traded', signal_S8, row, entities)
#END######################get signals from S8_LP_token_traded and append them to df_signals###################################################
                    #Add the addresses identified by the signalised identification process to identified_addresses and df_all_transactions
                    #get all transactions for the signalised addresses
                    try:       
                        new_transactions_signalised = get_extra_transactions(current_entity_name,signal_identified_pairs, filter_from_date)
                        logger.info("\n")
                        #logger.info("the shape of new_transactions_signalised is %s", new_transactions_signalised.shape)
                        #concat the main df with the new df and drop duplicates 
                        combined_df_signalised = pd.concat([df_all_transactions, new_transactions_signalised])
                        #logger.info("the shape of combined_df is %s", combined_df_signalised.shape)
                        combined_df_signalised.drop_duplicates(subset=['id', 'tx_direction'], keep='first', inplace=True)
                        #store as main df
                        df_all_transactions = combined_df_signalised
                        logger.info("Cumulative number of transactions: %s", df_all_transactions.shape[0])

                    except TypeError as te:
                        logger.error(f"A TypeError occurred: {str(te)}")
                        # Optionally, you can provide more context information about the error:
                        logger.debug("Detailed traceback:", exc_info=True)

                    except Exception as e:
                        logger.error(f"An unexpected error occurred: {str(e)}")
                        # Optionally, you can provide more context information about the error:
                        logger.debug("Detailed traceback:", exc_info=True)
##########################################################end the signaling process########################################################################
                logger.info(f"Found {len(df_new_txs)} new transactions for address {address}. Updated timestamp.")



########################## Store the signals in a csv file in appending mode for analysis purposes######################################################
            # Specify the full path to the file in the mounted Azure File Share
            logger.info("Writing to files starts...")
            if os.environ.get("RUNNING_IN_ACI"):  # You set this environment variable only in your Azure container
                filename = filename = '/mnt/data/df_signals.csv'
            else:
                filename = 'mnt/data/df_signals.csv'  # Replace with appropriate local path
            logger.info(f"Filename determined: {filename}")

            # Check if the file exists
            file_exists = os.path.isfile(filename)
            if file_exists:
                logger.info(f"File {filename} exists. Appending to the existing file.")
            else:
                logger.info(f"File {filename} does not exist. A new file will be created.")

            # Save the df_signals DataFrame to a CSV file
            try:
                df_signals.to_csv(filename, mode='a', header=not file_exists, index=False)
                logger.info(f"Successfully saved df_signals to {filename}.")
            except Exception as e:
                logger.error(f"Error while saving df_signals to {filename}: {e}")


            #logger.info("Updated state:")
            #logger.info("last_seen_tx_timestamps: %s", last_seen_tx_timestamps)
            logger.info(f"df_all_transactions shape: {df_all_transactions.shape[0]}")  # Print the shape of the DataFrame

        else:
            #logger.info(f"No new transactions found for address {address}.")
            pass


    # Wait for sleep_time seconds before polling again
    #logger.info(f"\nWaiting for {sleep_time} seconds before next check...")
    time.sleep(sleep_time)

    return df_all_transactions, identified_addresses



