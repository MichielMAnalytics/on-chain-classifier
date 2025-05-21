import requests
import pandas as pd
from variables import entities
import logging
logger = logging.getLogger(__name__)
from settings import get_config, url_transfers, headers

'''
This script is designed to be run as a standalone script. When you run python root_identification.py, it will execute the main() function. However, you can also import this module into another script and call its functions individually, like so:

'''

def get_transfers(base_contract_address):
    logger.info("making a call to the transfers endpoint...")
    response = requests.get(url_transfers, params={'base': base_contract_address}, headers=headers)
    
    if response.status_code == 200:
        logger.info('Succesfully called the transfers endpoint')
    else:
        raise Exception(f'Error: {response.content}')

    return response.json()

def extract_entity_root(transfers,root_filters):
    df = pd.json_normalize(transfers, 'transfers', sep='_')

    #get the filters from the dictionary
    root_filter_1_toIsContract = root_filters.get('toIsContract')
    root_filter_2_fromAddressArkhamLabel = root_filters.get('fromAddressArkhamLabel')
    root_filter_3_fromAddressArkhamEntityType = root_filters.get('fromAddressArkhamEntityType')
    root_filter_4_unitValue = root_filters.get('unitValue')


    root_filter_1 = df['toIsContract'] == root_filter_1_toIsContract if root_filter_1_toIsContract is not None else pd.Series(True, index=df.index)
    root_filter_2 = df['fromAddress_arkhamLabel_name'].isin(root_filter_2_fromAddressArkhamLabel) if root_filter_2_fromAddressArkhamLabel is not None else pd.Series(True, index=df.index)
    root_filter_3 = df['fromAddress_arkhamEntity_type'].isna() if root_filter_3_fromAddressArkhamEntityType is not None else pd.Series(True, index=df.index)
    root_filter_4 = df['unitValue'] == root_filter_4_unitValue if root_filter_4_unitValue is not None else pd.Series(True, index=df.index)

    entity_root_tx = df[root_filter_1 & root_filter_4 & root_filter_2 & root_filter_4]

     # More diagnostics
    print(f"Filtered DataFrame Results: {len(entity_root_tx)}")
    print(entity_root_tx.head())  # Print the top 5 rows of entity_root_tx

    # Check the number of rows in entity_root_tx
    if len(entity_root_tx) > 1:
        logger.warning("Multiple rows found for entity root, taking the first row by default.")
    elif len(entity_root_tx) == 0:
        raise ValueError("No rows found for entity root based on the applied filters.")

    # Using .iloc[0] to fetch the first value if multiple rows exist
    entity_root = entity_root_tx['toAddress_address'].iloc[0]
    entity_tx_hash = entity_root_tx['transactionHash'].iloc[0]
    entity_root_historicalUSD = entity_root_tx['historicalUSD'].iloc[0]

    return entity_root, entity_tx_hash, entity_root_historicalUSD

def main():
    for entity in entities:
            base_contract_address = entity['base_contract_address']
            # Get the transfers
            transfers = get_transfers(base_contract_address)
            entity_root, entity_tx_hash, entity_root_historicalUSD = extract_entity_root(transfers, entity['root_filters']) 
            return entity_root, entity_tx_hash, entity_root_historicalUSD

if __name__ == "__main__":
    main()


