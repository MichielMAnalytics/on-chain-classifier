#for the additional identification conditions (the first ETH IN transaction is always captured)
pct_total_value_threshold = 20000000 #not active
no_of_in_interactions_threshold = 500000000 #not active
no_of_out_interactions_threshold = 20000000 #not active
USD_tx_threshold = 20000000000000000000000000000 #not active

#essential to change if you start from either a single address or from a Maple loan
single_address_input = True

entities = [
#  {'base_contract_address': 'your contract address',
#     'entity_name': 'Maven 11',
#     'entity_tx_hash': 'your tx hash',
#     'historicalUSD': 100000, #not being used
#     'entities_to_include': {
#         'incoming_entities_to_include': ['Maven 11','maven-11', 'Maple Loan Proxy'],
#         'outgoing_entities_to_include': ['Maven 11','maven-11']
#     },
#     'filter_from_date': '2015-01-01',
#     'arkham_entity': 'maven-11',
#     'AOM_ballpark': 80000000
#     },

#  {'base_contract_address': 'your contract address',
#     'entity_name': 'your entity name',
#     'entity_tx_hash': 'your tx hash',
#     'historicalUSD': 100000, #not being used,
#     'entities_to_include': {
#         'incoming_entities_to_include': ['Orthogonal Trading','orthogonal-trading', 'Maple Loan Proxy'],
#         'outgoing_entities_to_include': ['Orthogonal Trading','orthogonal-trading']
#     },
#     'filter_from_date': '2015-01-01',
#     'arkham_entity': 'orthogonal-trading',
#     'AOM_ballpark': 108000000
#     },

    # {
    # 'base_contract_address': 'your contract address',
    # 'entity_name': 'your entity name',
    # 'root_filters': {
    #     'toIsContract': False,
    #     'fromAddressArkhamLabel': ['Maple Loan Proxy', '"Maven11 WETH Pool'],
    #     'fromAddressArkhamEntityType': None,
    #     'unitValue': 1000
    # },
    # 'entities_to_include': {
    #     'incoming_entities_to_include': ['Portofino', 'portofino-technologies','Maple Loan Proxy'],
    #     'outgoing_entities_to_include': ['Portofino','portofino-technologies']
    # },
    # 'filter_from_date': '2021-04-01',
    # 'arkham_entity': 'portofino-technologies',
    # 'AOM_ballpark': 40000000
    # },

    {
    'base_contract_address': '0xEa1f2EA82faf44cbe2950322d094A7202ec8499E',
    'entity_tx_hash': '0x2741af0abe2dbf0cb2eb3bdf8e4aecc4d7023d65a2f20414ef8e629e33116106',
    'historicalUSD': 100000, #not being used,
    'entity_name': None,
    'root_filters': {
        'toIsContract': False,
        'fromAddressArkhamLabel': None,
        'fromAddressArkhamEntityType': None,
        'unitValue': 1000
    },
    'entities_to_include': {
        'incoming_entities_to_include': None,
        'outgoing_entities_to_include': None
    },
    'filter_from_date': '2025-01-01',
    'arkham_entity': None,
    'AOM_ballpark': 100000
    },

    # {
    # 'base_contract_address': 'your contract address',
    # 'entity_name': 'your entity name',
    # 'root_filters': {
    #     'toIsContract': False,
    #     'fromAddressArkhamLabel': None,
    #     'fromAddressArkhamEntityType': None,
    #     'unitValue': 4000,
    # },
    # 'entities_to_include': {
    #     'incoming_entities_to_include': ['Auros Global', 'auros','Maple Loan Proxy'],
    #     'outgoing_entities_to_include': ['Auros Global','auros']
    # },
    # 'filter_from_date': '2021-05-01',
    # 'arkham_entity': 'auros',
    # 'AOM_ballpark': 80000000 
    # },

    #other entities
]




