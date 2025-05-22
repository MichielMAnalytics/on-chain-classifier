import logging
logger = logging.getLogger(__name__)
import requests
import pandas as pd
from settings import (url_bot_telegram,BASE_URLS,
    ETHERSCAN_URL_TEMPLATE,
    ARKHAM_URL_TEMPLATE,
    no_of_days_ago_s1, S5_relative_threshold, get_config)
from supporting_functions import find_entity_by_name, save_addresses, get_usd_balance, logger_setup
from variables import entities
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler
from telegram import Bot
config_values = get_config()
bot_token = config_values['bot_token'] 
import asyncio
import nest_asyncio
from collections import defaultdict


def format_large_number(num):
    """
    This function takes a number and returns it in a more readable format.
    For numbers >= 1000, it adds suffixes: K, M, B, etc.
    """
    for unit in ['', 'K', 'M', 'B', 'T']:
        if abs(num) < 1000:
            formatted_number = f"{num:.2f}"
            # Remove trailing zeros after the decimal point and the decimal point if not needed
            if formatted_number.endswith('.00'):
                formatted_number = formatted_number[:-3]
            return f"{formatted_number}{unit}".strip()  # Removed space between number and unit
        num /= 1000.0
    return f"{num:.2f}P"  # returns in peta if > 1000 tera


def convert_nan_to_str(value):
    if pd.notna(value):
        return format_large_number(float(value))
    else:
        return 'NaN'


def signal_s1_value(value):
    total_usd_bool, total_usd, only_ethereum, activity_last_period = value
    total_usd = format_large_number(total_usd)
    
    if total_usd_bool and only_ethereum and activity_last_period: #possible but unlikely
        message = f"The wallet has been inactive for the past {no_of_days_ago_s1} days, only contains Ethereum, and its converted USD balance is ${total_usd} or less."
    elif total_usd_bool and only_ethereum: #impossible
        message = f"The wallet only contains Ethereum and its converted USD balance is ${total_usd} or less."
    elif total_usd_bool and activity_last_period: #possible
        message = f"The wallet has been inactive for the past {no_of_days_ago_s1} days, and its converted USD balance is ${total_usd} or less."
    elif only_ethereum and activity_last_period:#possible
        message = f"The wallet has been inactive for the past {no_of_days_ago_s1} days and only contains Ethereum with a converted USD balance of ${total_usd}."
    elif total_usd_bool: #impossible
        message = f"The wallet's converted USD balance is ${total_usd} or less."
    elif only_ethereum: #impossible
        message = f"The wallet only contains Ethereum with a converted USD balance of ${total_usd}."
    elif activity_last_period: #impossible
        message = f"The wallet has been inactive for the past {no_of_days_ago_s1} days."
    else:
        message = "No unusual activity detected."

    return message




def signal_s2_value(value):
    interacting_with_new_protocol, new_protocols, previous_interaction = value
    if interacting_with_new_protocol:
        new_protocols = ', '.join(new_protocols)
        if previous_interaction:
            message = f"The address is interacting with new protocols: {new_protocols}. There has been a previous interaction with these protocols."
        else:
            message = f"The address is interacting with new protocols: {new_protocols}. There has been no previous interaction with these protocols."
    else:
        message = "The address is not interacting with any new protocols."
    return message


def signal_s3_value(value):
    new_token, token_symbol = value
    if new_token:
        message = f"The address is interacting with a new cryptocurrency token: {token_symbol}."
    else:
        message = "The address is not interacting with any new cryptocurrencies."
    return message
    
def signal_s4_value(value):
    is_new_exchange, exchange_name = value # Unpack 2 values
    message = ""
    if is_new_exchange:
        # Based on the signal generation logic, is_new_exchange will be True when this function is called.
        message = f"The address is interacting with a new exchange: {exchange_name}."
    else:
        # This path should ideally not be reached if the signal is only added when is_new_exchange is True.
        # Adding a log for unexpected cases.
        logger.warning("signal_s4_value called with is_new_exchange as False, which is unexpected.")
        message = "Interaction with a new exchange was flagged, but details are inconsistent."
    return message

def signal_s5a_value(value, signal_row, entities):
    alert, daily_volume, threshold = value

    # Get the entity's details using the entity name from the signal_row
    entity_details = find_entity_by_name(entities, signal_row['entity_name'])

    aom_ballpark = entity_details['AOM_ballpark']

    message = ""

    if pd.isna(alert):
        message = "There's insufficient historical data to determine if the daily trading volume has exceeded the threshold."
    elif alert:
        message = (f"On this day, the trading volume of ${format_large_number(daily_volume)} exceeded the threshold of "
                   f"${format_large_number(threshold)}. This threshold is based on an AOM ballpark of ${format_large_number(aom_ballpark)} and a "
                   f"relative threshold of {S5_relative_threshold * 100}%.")
    else:
        message = (f"On this day, the trading volume of ${format_large_number(daily_volume)} did not exceed the threshold of "
                   f"${format_large_number(threshold)}. This threshold is based on an AOM ballpark of ${format_large_number(aom_ballpark)} and a "
                   f"relative threshold of {S5_relative_threshold * 100}%.")

    return message


def signal_s5b_value(value, signal_row, entities):
    alert, tx_volume, threshold = value

    # Get the entity's details using the entity name from the signal_row
    entity_details = find_entity_by_name(entities, signal_row['entity_name'])

    aom_ballpark = entity_details['AOM_ballpark']

    message = ""

    if pd.isna(alert):
        message = "There's insufficient historical data to determine if the daily trading volume has exceeded the threshold."
    elif alert:
        message = (f"The transaction volume of ${format_large_number(tx_volume)} exceeded the threshold of "
                   f"${format_large_number(threshold)}. This threshold is based on an AOM ballpark of ${format_large_number(aom_ballpark)} and a "
                   f"relative threshold of {S5_relative_threshold * 100}%.")
    else:
        message = (f"The transaction volume of ${format_large_number(tx_volume)} did not exceed the threshold of "
                   f"${format_large_number(threshold)}. This threshold is based on an AOM ballpark of ${format_large_number(aom_ballpark)} and a "
                   f"relative threshold of {S5_relative_threshold * 100}%.")

    return message


def signal_s6a_value(value):
    flag, last_day_change, freq_mean, freq_std = value

    # Convert the float values to strings with 2 decimal places for better readability in the message
    last_day_change_str = "{:.2f}".format(last_day_change)
    freq_mean_str = "{:.2f}".format(freq_mean)
    freq_std_str = "{:.2f}".format(freq_std)

    message = ""

    if flag:
        message = f"The change in daily transaction frequency on the last day ({last_day_change_str}) is greater than the defined number of standard deviations ({freq_std_str}) from the mean frequency ({freq_mean_str})."
    else:
        message = f"The change in daily transaction frequency on the last day ({last_day_change_str}) is not greater than the defined number of standard deviations ({freq_std_str}) from the mean frequency ({freq_mean_str})."

    return message


def signal_s6b_value(value):
    flag, last_week_change, freq_mean, freq_std = value

    # Convert the float values to strings with 2 decimal places for better readability in the message
    last_week_change_str = "{:.2f}".format(last_week_change)
    freq_mean_str = "{:.2f}".format(freq_mean)
    freq_std_str = "{:.2f}".format(freq_std)

    message = ""

    if flag:
        message = f"The change in weekly transaction frequency on the last week ({last_week_change_str}) is greater than the defined number of standard deviations ({freq_std_str}) from the mean frequency ({freq_mean_str})."
    else:
        message = f"The change in weekly transaction frequency on the last week ({last_week_change_str}) is not greater than the defined number of standard deviations ({freq_std_str}) from the mean frequency ({freq_mean_str})."

    return message

def signal_s7_value(value):
    flag, changes, prev_balances, current_balances = value
    
    if flag:
        changes_str = []
        for protocol, change in changes.items():
            prev_balance = prev_balances.get(protocol, 0)
            current_balance = current_balances.get(protocol, 0)
            changes_str.append(f"{protocol}: {change:.2f}% (Previous balance: {format_large_number(prev_balance)}, Current balance: {format_large_number(current_balance)})")
        
        changes_str = ", ".join(changes_str)
        message = f"The transaction shows significant protocol balance changes for the following protocols: {changes_str}"
    else:
        message = "The transaction does not show any significant protocol balance changes."

    return message

def signal_s8_value(value):
    lp_present, token_info = value
    
    if lp_present:
        message = f"The transaction involves a Liquidity Pool (LP) token, identified by '{token_info}'. This indicates participation in liquidity provision."
    else:
        message = "The transaction does not involve any Liquidity Pool (LP) tokens, suggesting no liquidity provision activity in this transaction."

    return message




signal_value_transforms = {
    'S1 Fresh Wallet': signal_s1_value,
    'S2 Interaction New Protocol': signal_s2_value,
    'S3 Interaction New Crypto': signal_s3_value,
    'S4 Interaction New Exchange': signal_s4_value, 
    'S5a Daily Cumulative Volume Exceeded': signal_s5a_value,
    'S5b Daily Absolute Volume Exceeded': signal_s5b_value,
    'S6a Daily Frequency Change': signal_s6a_value,
    'S6b Weekly Frequency Change': signal_s6b_value,
    'S7 Protocol Activity': signal_s7_value,
    'S8 LP token traded': signal_s8_value

}

def construct_alert_message(signal_row):

    #make a copy
    signal_row = signal_row.copy() 
    
    import re

    def escape_markdown_special_chars(text):
        # List of special characters that need to be escaped in markdown, except parentheses
        special_chars = ['_', '*', '[', ']', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}',  '!'] #'.',
        
        for char in special_chars:
            text = text.replace(char, f'\\{char}')

        return text

    # Now use this function in your loop:

    for column in signal_row.index:
        if isinstance(signal_row[column], str):
            if column == 'historicalUSD' or column == 'unitValue':
                # Convert to float and round before formatting the string
                signal_row[column] = '{:.2f}'.format(float(signal_row[column]))
            elif column != 'Timestamp': #added this
                signal_row[column] = escape_markdown_special_chars(signal_row[column])

    # Transform Signal_Value based on Signal_Type
    transform_func = signal_value_transforms.get(signal_row['Signal_Type'], lambda x: x)  # Default to identity function if Signal_Type not found
    if (signal_row['Signal_Type'] == 'S5a Daily Cumulative Volume Exceeded') or (signal_row['Signal_Type'] == 'S5b Daily Absolute Volume Exceeded'):
        transformed_value = transform_func(signal_row['Signal_Value'], signal_row, entities)
    else:
        transformed_value = transform_func(signal_row['Signal_Value'])



    # Construct the hyperlinks
    hyperlink_etherscan = ETHERSCAN_URL_TEMPLATE.format(transaction_hash=signal_row["transactionHash"])
    hyperlink_arkham = ARKHAM_URL_TEMPLATE.format(transaction_hash=signal_row["transactionHash"])

     # Extract shortened parts
    short_interacting_address = f"{signal_row['interacting_address'][:5]}"
    short_root_address = f"{signal_row['root_address'][:5]}"
    short_transaction_hash = f"{signal_row['transactionHash'][:5]}"

    def get_explorer_link(chain, endpoint, identifier):
        """
        Generates the full URL for a given chain, endpoint (address or tx), and identifier.
        """
        base_url = BASE_URLS.get(chain, '')  # Default to empty string if chain not found
        return f"{base_url}{endpoint}{identifier}"

    # Function to get explorer name for the chain
    def get_explorer_name(chain):
        mapping = {
            'ethereum': 'Etherscan',
            'polygon': 'Polygonscan',
            'avalanche': 'Avalanche Explorer',
            'bsc': 'Bscscan',
            'arbitrum_one': 'Arbiscan',
            'optimism': 'Optimistic Etherscan',
            'base': 'Basescan',
        }
        return mapping.get(chain, 'Etherscan')

    # # Construct the hyperlinks based on the chain
    # short_hyperlink_root = f"[{signal_row['root_address'][:5]}]({get_explorer_link(signal_row['chain'], signal_row['root_address'])})"
    # short_hyperlink_interacting = f"[{signal_row['interacting_address'][:5]}]({get_explorer_link(signal_row['chain'], signal_row['interacting_address'])})"
    # short_hyperlink_hash = f"[{signal_row['transactionHash'][:5]}]({get_explorer_link(signal_row['chain'], 'tx/' + signal_row['transactionHash'])})"

    short_hyperlink_interacting = f"[{short_interacting_address}]({get_explorer_link(signal_row['chain'], 'address/', signal_row['interacting_address'])})"
    short_hyperlink_root = f"[{short_root_address}]({get_explorer_link(signal_row['chain'], 'address/', signal_row['root_address'])})"
    short_hyperlink_hash = f"[{short_transaction_hash}]({get_explorer_link(signal_row['chain'], 'tx/', signal_row['transactionHash'])})"



    def replace_nan_with_unknown(label):
        if pd.isna(label) or str(label).lower() == 'nan':
            return 'Unknown'
        else:
            # Replace standalone 'nan' or ' (nan)' with 'Unknown' or '', respectively
            label = re.sub(r'\bnan\b', 'Unknown', label, flags=re.IGNORECASE)
            label = re.sub(r'\s*\(Unknown\)', '', label, flags=re.IGNORECASE)
            return label

    signal_row['to_name_label'] = replace_nan_with_unknown(signal_row['to_name_label'])
    signal_row['from_name_label'] = replace_nan_with_unknown(signal_row['from_name_label'])

    from_name_label = '' if signal_row['from_name_label'] == 'Unknown' else f" ({signal_row['from_name_label']})"
    to_name_label = '' if signal_row['to_name_label'] == 'Unknown' else f" ({signal_row['to_name_label']})"

    # Decide who is sender and receiver based on transaction direction
    if signal_row['tx_direction'] == 'OUT':
        sender = f"{signal_row['entity_name']}{from_name_label} ({short_hyperlink_root})" #we can add from_name_label here as well on top of the manual input [ADDED: test]
        receiver =  f"{signal_row['to_name_label']} ({short_hyperlink_interacting})"
    else:
        sender = f"{signal_row['from_name_label']} ({short_hyperlink_interacting})"
        receiver = f"{signal_row['entity_name']}{to_name_label} ({short_hyperlink_root})" #we can add _tolabel_name here as well on top of the manual input [ADDED: test]


    # Construct the message
    message = (
        f"``` 🔔 New Transaction Alert 🔔 ```\n\n"
        f"*Type:* {signal_row['Signal_Type']}\n"
        f"*Signal:* {transformed_value}\n"
        f"*From:* {sender}\n"
        f"*To:* {receiver}\n"
        f"*Direction:* {signal_row['tx_direction']}\n"
        f"*Value:* {format_large_number(signal_row['unitValue'])} {signal_row['tokenSymbol']} (${format_large_number(signal_row['historicalUSD'])})\n"
        f"*Network:* {signal_row['chain']}\n"
        f"*Time:* {signal_row['Timestamp']}\n" 
        f"*Transaction Hash:* {short_hyperlink_hash}\n\n"
        f"[View on {get_explorer_name(signal_row['chain'])}]({get_explorer_link(signal_row['chain'], 'tx/', signal_row['transactionHash'])}) | {hyperlink_arkham}\n\n"

    )
    return message

def send_telegram_message(bot_token, chat_id, message,root_address):
    url = url_bot_telegram + bot_token + '/sendMessage'

    # Creating a button for stopping monitoring
    stop_button = InlineKeyboardButton(f"Stop Alerts for Address {root_address}", callback_data=f'stop_{root_address}')
    custom_keyboard = InlineKeyboardMarkup([[stop_button]]).to_json()
    
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown',
        'reply_markup': custom_keyboard
   
    }
    headers = {'content-type': 'application/x-www-form-urlencoded'}

    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.info(f"HTTP Error: {errh}")
        return False
    except requests.exceptions.ConnectionError as errc:
        logger.info(f"Error Connecting: {errc}")
        return False
    except requests.exceptions.Timeout as errt:
        logger.info(f"Timeout Error: {errt}")
        return False
    except requests.exceptions.RequestException as err:
        logger.info(f"Something went wrong: {err}")
        return False

    # check if the request was successful
    if response.json()["ok"]:
        logger.info("Message sent successfully") 
        return
    else:
        logger.info(f"Telegram API returned an error: {response.json()['description']}")
        return False
    
async def button(update, context,removal_queue):
    query = update.callback_query
    await query.answer()
    root_address = query.data.split('_')[1]
    # Instead of calling stop_monitoring directly, put root_address in the removal_queue
    removal_queue.put(root_address)
    logger.info(f"Added {root_address} to removal_queue. Queue size: {removal_queue.qsize()}")

    # Send a new message
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=f'Alerts stopped for address: {root_address}',
        reply_to_message_id=query.message.message_id  # This line makes the new message a reply to the original message
     )


def stop_monitoring(removal_queue, all_transactions_df, all_identified_addresses):
    logger.info("stop_monitoring function called")
    logger.info(f"removal_queue size: {removal_queue.qsize()}")

    # Initialize removed_tx_counts_dict
    removed_tx_counts_dict = defaultdict(int)

    while not removal_queue.empty():
        try:
            root_address = removal_queue.get()
            logger.info(f"Processing removal for address {root_address}")

            # Record the initial counts
            initial_address_count = len(all_identified_addresses)
            initial_transaction_count = len(all_transactions_df)

            # Extract transactions related to root_address
            removed_transactions_df = all_transactions_df.loc[all_transactions_df['root_address'] == root_address]
            
            # Add the removed transaction counts to the dictionary
            entity_counts = removed_transactions_df.groupby('entity_name').size().to_dict()
            for entity, count in entity_counts.items():
                removed_tx_counts_dict[entity] += count

            # Attempt to remove the address from all_identified_addresses
            updated_identified_addresses = [t for t in all_identified_addresses if t[0] != root_address]
            if len(updated_identified_addresses) == len(all_identified_addresses):
                logger.warning(f"No matching address found in all_identified_addresses for {root_address}")

            # Extract and save the removed addresses before updating all_identified_addresses
            removed_addresses = [t for t in all_identified_addresses if t[0] == root_address]
            save_addresses(removed_addresses, status="removed by user")

            #update the original tuples list
            all_identified_addresses = updated_identified_addresses

            # Attempt to remove transactions from all_transactions_df
            updated_transactions_df = all_transactions_df.loc[all_transactions_df['root_address'] != root_address]
            if len(updated_transactions_df) == len(all_transactions_df):
                logger.warning(f"No transactions found for address {root_address}")
            all_transactions_df = updated_transactions_df

            # Log the count of removed addresses and transactions
            removed_address_count = initial_address_count - len(all_identified_addresses)
            removed_transaction_count = initial_transaction_count - len(all_transactions_df)
            logger.info(f"Removed {removed_address_count} addresses and {removed_transaction_count} transactions for address {root_address}")


            # Extract and save the removed addresses before deletion
            removed_addresses = [t for t in all_identified_addresses if t[0] == root_address]
            save_addresses(removed_addresses, status="removed by user")
            
            logger.info(f"Removal processed for address {root_address}")

        except Exception as e:
            logger.error(f"An error occurred while processing removal for address {root_address}: {e}")

    return all_transactions_df, all_identified_addresses, removed_tx_counts_dict

def telegram_polling(removal_queue):

     # Specify the log file path for the Telegram polling process
    telegram_log_path = '/mnt/data/telegram.log'

    # Create a logger for the Telegram polling process using the specified log file path
    logger = logger_setup(telegram_log_path)

    async def run_telegram_polling():
        try:
            logger.info("Starting Telegram polling")
            app = Application.builder().token(bot_token).build()
            button_handler = CallbackQueryHandler(lambda u, c: button(u, c, removal_queue))
            app.add_handler(button_handler)
            await app.run_polling()
        except Exception as e:
            logger.error(f"Error in run_telegram_polling: {e}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)

    try:
        loop.run_until_complete(run_telegram_polling())
    finally:
        loop.close()



def send_telegram_message_basic(bot_token, chat_id, message):
    url = url_bot_telegram + bot_token + '/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    headers = {'content-type': 'application/x-www-form-urlencoded'}

    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.info(f"HTTP Error: {errh}")
        return False
    except requests.exceptions.ConnectionError as errc:
        logger.info(f"Error Connecting: {errc}")
        return False
    except requests.exceptions.Timeout as errt:
        logger.info(f"Timeout Error: {errt}")
        return False
    except requests.exceptions.RequestException as err:
        logger.info(f"Something went wrong: {err}")
        return False

    # check if the request was successful
    if response.json()["ok"]:
        logger.info("Message sent successfully") 
        return
    else:
        logger.info(f"Telegram API returned an error: {response.json()['description']}")
        return False


def send_daily_report(df_all_transactions, identified_addresses, prev_tx_counts, bot_token, chat_id):
    quote = get_daily_quote()

    quote_message = f"\n\n*Today's Wisdom:* \n{quote}" if quote else ""

    # Initialize the message header
    message = f"``` 🍾 Daily Report 🍾 ```\n\n"

    # Group by entity and compute metrics
    grouped_entities = df_all_transactions.groupby('entity_name')
    for entity_name, group in grouped_entities:
        
        # Calculate total transactions for the entity
        entity_tx_count = group.shape[0]
        
        # Calculate new transactions for the entity today
        entity_new_tx_today = entity_tx_count - prev_tx_counts.get(entity_name, 0)
        
        # Update the previous transaction count for the entity
        prev_tx_counts[entity_name] = entity_tx_count
        
        # Calculate identified addresses for the entity
        entity_identified_addresses = [address for address, _, entity, _ in identified_addresses if entity == entity_name]
        num_entity_identified_addresses = len(set(entity_identified_addresses))  # Convert to set to count unique addresses

        # Calculate identified addresses for the entity
        entity_identified_addresses = [address for address, _, entity, _ in identified_addresses if entity == entity_name]
        
        # Calculate 'Assets under management' for the entity by summing the USD balance of each identified address
        entity_aum = sum([get_usd_balance(address) for address in entity_identified_addresses])
        entity_aum = format_large_number(entity_aum)  # Format the number for readability


        message += f"*Entity:* {entity_name}\n" \
                    f"*Total identified addresses:* {num_entity_identified_addresses}\n" \
                    f"*Total number of transactions:* {entity_tx_count}\n" \
                    f"*Transactions today:* {entity_new_tx_today} \n" \
                    f"*Assets under management:* ${entity_aum}\n\n" 

    # Append the daily wisdom quote to the message
    message += quote_message

    send_telegram_message_basic(bot_token, chat_id, message)
    return prev_tx_counts

def get_daily_quote():
    try:
        response = requests.get("https://api.quotable.io/random")
        if response.status_code == 200:
            data = response.json()
            quote = data["content"]
            author = data["author"]
            return f'"{quote}" - {author}'
        else:
            return None
    except Exception as e:
        print(f"Error fetching quote: {e}")
        return None
    

def send_start_system(bot_token, chat_id):
    # Link to your GIF, this is just a placeholder and might not work. 
    # Ensure you replace it with a valid link to a rocket GIF.
    gif_url = "https://drive.google.com/uc?export=view&id=1iC_dij2f8T7ImoZb48pHb0qNllEWDmJV" 
    
    # Send the message
    send_telegram_gif(bot_token, chat_id, gif_url)

def send_telegram_gif(bot_token, chat_id, gif_url):
    base_url = "https://api.telegram.org/bot"
    send_gif_url = f"{base_url}{bot_token}/sendDocument"
    payload = {
        'chat_id': chat_id,
        'document': gif_url,
        'caption': "🚀 The system has been started! 🚀"
    }
    headers = {'content-type': 'application/x-www-form-urlencoded'}

    try:
        response = requests.post(send_gif_url, data=payload, headers=headers)
        response.raise_for_status()
    except requests.RequestException as err:
        # Handle all possible exceptions here
        logger.info(f"Something went wrong: {err}")
        return False

    # Check if the request was successful
    if response.json()["ok"]:
        logger.info("GIF sent successfully")
        return True
    else:
        logger.info(f"Telegram API returned an error: {response.json()['description']}")
        return False


