'''
Set up:
- main.py (all once to run lines can be in here)

- Imports & Settings
- Variable Setting
- Entity root identification
- Address identification
    - identification functions
- Monitoring and alert generating
    - Signal functions
    - Messaging functions
'''
import pandas as pd
from datetime import datetime
import time
import multiprocessing
#from queue import Queue

from variables import entities, single_address_input
from supporting_functions import filter_transactions_and_addresses, add_known_entity_transactions, save_addresses, load_state, save_state,append_state, get_and_log_env_variables, logger_setup
from settings import get_config
from messaging_functions import send_daily_report, send_start_system
from root_identification import get_transfers, extract_entity_root
from address_identification import address_identification
from monitoring_and_alert_generation import initialize_monitoring, monitor_transactions
from messaging_functions import telegram_polling, stop_monitoring
from collections import defaultdict


config_values = get_config()
api_key = config_values['api_key']
bot_token = config_values['bot_token'] 
chat_ID = config_values['chat_ID']


####################################################################################################################################################################
if __name__ == "__main__":
    main_log_path = '/mnt/data/app.log'  # Main process log file
    logger = logger_setup(main_log_path)
    logger.info("Main Script started!")


    #set the common queue for the removals
    manager = multiprocessing.Manager()
    removal_queue = manager.Queue()  # Now removal_queue is a managed queue

    # Create and start a process for the Telegram polling logic
    telegram_process = multiprocessing.Process(target=telegram_polling, args=(removal_queue,))
    telegram_process.start()


    #print and get relevant environment variables
    env_values = get_and_log_env_variables(logger)
    # Now you can use env_values as needed in your script
    start_from_state = env_values['START_FROM_STATE']

    if not start_from_state:
        # Cumulative Dataframe and list for all entities
        all_transactions_df = pd.DataFrame()
        all_identified_addresses = []

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

            # Add all transactions with known Arkham Label (e.g. Portofino Technologies) to df_all_transactions and the addresses and transactionHashes to identified_addresses
            logger.info(f"Adding known entity transactions for entity: {entity_name}")
            df_all_transactions, identified_addresses = add_known_entity_transactions(df_all_transactions, identified_addresses, entity_name, arkham_entity) 

            # Append data to the cumulative dataframe and list
            logger.info(f"Appending data for entity: {entity_name}")
            all_transactions_df = all_transactions_df.append(df_all_transactions, ignore_index=True)
            all_identified_addresses.extend(identified_addresses)

            #delete df_all_transactions to save memory
            logger.info(f"Clearing temporary data for entity: {entity_name}")
            del df_all_transactions, identified_addresses

    else:
        all_transactions_df, all_identified_addresses = load_state()
        if all_transactions_df is None or all_identified_addresses is None:
            logger.error("Failed to load state, cannot continue.")
            # Handle error, e.g., exit script
            exit(1)

    # Initialize monitoring
    logger.info("Initializing monitoring.")
    init_values = initialize_monitoring(all_transactions_df)

    # To track the count of transactions from the previous day (used for the daily report)
    grouped_entities = all_transactions_df.groupby('entity_name')
    #initialize the tx counts with 0
    prev_tx_counts = {entity['entity_name']: 0 for entity in entities}
    #populate with the shape
    for entity_name, group in grouped_entities:
        prev_tx_counts[entity_name] = group.shape[0]
    

    # Send a start message
    #logger.info("Sending system start notification.")
    #send_start_system(bot_token, chat_ID)



    #save the identified addresses, both csv and pkl format, and the transactions
    append_state(all_transactions_df, all_identified_addresses) 
    save_addresses(all_identified_addresses) #should be removed eventually

    #save the lengths to know whether to save a new state or not
    previous_tx_df_length = len(all_transactions_df)
    previous_id_addresses_length = len(all_identified_addresses)

    # Monitor transactions in a while loop
    logger.info("Starting transaction monitoring loop.")

    while True:


        current_time = datetime.now().strftime('%H:%M')
        if "17:00" <= current_time < "17:02": # needed to add two min because of calculating AOM
            prev_tx_counts = send_daily_report(all_transactions_df, all_identified_addresses, prev_tx_counts, bot_token, chat_ID)
            # Sleep for a minute to avoid sending the message multiple times at 00:00
            time.sleep(120)

        # Reset removed_tx_counts after sending the daily report
        removed_tx_counts = defaultdict(int)

        #identified_addresses moet ook gereturned worden
        logger.info("Monitoring transactions.")
        all_transactions_df, all_identified_addresses = monitor_transactions(all_transactions_df, all_identified_addresses, *init_values)

        # Check the removal_queue in your main loop and call stop_monitoring whenever there's something in the queue
        if not removal_queue.empty():
            logger.info(f"removal_queue size just before removing: {removal_queue.qsize()}")
            all_transactions_df, all_identified_addresses, removed_tx_counts_dict = stop_monitoring(removal_queue,all_transactions_df, all_identified_addresses)
            for entity_name, count in removed_tx_counts_dict.items():
                removed_tx_counts[entity_name] += count
            
            # Update prev_tx_counts after the stop_monitoring function call.
            for entity_name in removed_tx_counts_dict:
                prev_tx_counts[entity_name] -= removed_tx_counts_dict[entity_name]  # Subtract the count of removed transactions.


        # At some point where you want to save state:
        if len(all_transactions_df) != previous_tx_df_length or len(all_identified_addresses) != previous_id_addresses_length:
            save_state(all_transactions_df, all_identified_addresses)

            #save the identified addresses and append them with newly identified ones
            save_addresses(all_identified_addresses) #should be removed eventually

            # Update the lengths for the next iteration
            previous_tx_df_length = len(all_transactions_df)
            previous_id_addresses_length = len(all_identified_addresses)

