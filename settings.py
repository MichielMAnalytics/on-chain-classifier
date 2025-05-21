#import configparser
import pandas as pd
import os


#to load in environment variables from the secrets.env file
from dotenv import load_dotenv

# Load .env file only if not in Azure Container Instance
if not os.environ.get("RUNNING_IN_ACI"):
    load_dotenv()

def get_config():
    config_values = {
        'api_key': os.getenv('API_KEY', None),
        'api_key_debank': os.getenv('DEBANK_API_KEY', None),
        'chat_ID': os.getenv('CHAT_ID', None),
        'bot_token': os.getenv('BOT_TOKEN', None)
    }
    return config_values


config_values = get_config()
api_key = config_values['api_key']

# set up the API endpoint URL
url_transfers = 'https://api.arkhamintelligence.com/transfers'

def get_address_history_url(address):
    return f"https://api.arkhamintelligence.com/history/address/{address}"

url_bot_telegram = 'https://api.telegram.org/bot'


url_debank_complex_protocol = "https://pro-openapi.debank.com/v1/user/all_complex_protocol_list"

url_coingecko = "https://api.coingecko.com/api/v3/simple/price"

ETHERSCAN_URL_TEMPLATE = '[View on Etherscan](https://etherscan.io/tx/{transaction_hash})'
ARKHAM_URL_TEMPLATE = '[View on Arkham](https://platform.arkhamintelligence.com/explorer/tx/{transaction_hash})'


BASE_URLS = {
    'ethereum': 'https://etherscan.io/',
    'polygon': 'https://polygonscan.com/',
    'avalanche': 'https://subnets.avax.network/c-chain/',
    'bsc': 'https://bscscan.com/',
    'arbitrum_one': 'https://arbiscan.io/',
    'optimism': 'https://optimistic.etherscan.io/',
    'base': 'https://basescan.org/',
}



# set up the API headers with the API key
headers = {'API-Key': api_key}



#time in between the main loop of the system
sleep_time = 10

#threshold for transactions to be considered
usd_threshold = 50

#for S1 the period to check whether there were transactions
no_of_days_ago_s1 = 90

#threshold for exchange deposit to be considered as a deposit address
threshold_exchange_deposit = 5000

#threshold for exchange deposit in usd volume to be considered as a previous sender (high threshold)
threshold_exchange_deposit_USD_amount = 20000

#the thresholds that need to be exceeded for the signals to hit
num_std_dev_S6a_d = 3
num_std_dev_S6b_w = 3

#For S5a and b the threshold, relative to the AOM_ballpark, of a transaction value to pass to generate a trigger. 
S5_relative_threshold = 0.15 #0.05 * 40M = 2M

#no. of transactions processed at a time
limit = 1000

