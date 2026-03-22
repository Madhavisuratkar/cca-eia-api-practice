import configparser
import logging
import os
from datetime import datetime, timedelta
import pandas as pd
config = configparser.ConfigParser()
last_autosave_time = datetime.now()
ignore_path = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'connections')


def parse_datetime(datetime_str):
    """Parse datetime string to datetime object. Handles both with and without microseconds."""
    try:
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S,%f').replace(microsecond=0)
    except ValueError:
        try:
            return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.error(f'Exception in parsing datetime: {str(e)}')
            return None


def common_load_last_datetime(ini_file):
    """Loads the last datetime based on the advisor type from the configuration."""
    default_datetime = datetime(2025, 1, 30, 0, 0, 0)
    last_datetime = default_datetime

    if ini_file and os.path.exists(ini_file):
        config.read(ini_file)

    if 'app' in config and 'last_datetime' in config['app']:
        last_datetime_str = config['app']['last_datetime']
        last_datetime = parse_datetime(last_datetime_str) or default_datetime

    logging.info(f"Loaded last datetime: {last_datetime}")
    return last_datetime


def save_last_datetime(position, ini_file):
    """Save the last datetime for either EIA or CCA based on the category provided."""
    try:
        config.read(ini_file)
        config['app']['last_datetime'] = position.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Updated last datetime for {position}")
        with open(ini_file, 'w') as configfile:
            config.write(configfile)

        logging.info("Saved datetime to logger_config.ini.")
    except Exception as e:
        logging.error(f"Error saving datetime for {e}")


def autosave(last_datetime, ini_file):
    """Autosave the position file if the interval has elapsed."""
    global last_autosave_time
    if (datetime.now() - last_autosave_time) >= timedelta(seconds=1):
        last_autosave_time = datetime.now()
        save_last_datetime(last_datetime, ini_file)
    return last_autosave_time


def process_datetime_columns(df):
    """Splits a datetime column in the DataFrame into separate date and time columns."""
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df['date'] = df['datetime'].dt.date
        df['time'] = df['datetime'].dt.time
    return df


def extract_user_info(email):
    """Extract user and organization info from email."""
    if email:
        if "@" in email:
            user_name, domain = email.split('@', 1)
            organisation = domain.split('.')[0]
        else:
            user_name = email
            organisation = "amd"

        if organisation == 'amd':
            return user_name, organisation, user_name, None
        else:
            return user_name, organisation, None, user_name
    return None, None, None, None


def convert_memory_to_numeric(memory_str):
    """Convert memory size strings to numeric values (in GiB)."""
    try:
        if memory_str.endswith('Gi'):
            return float(memory_str[:-2])
        elif memory_str.endswith('Mi'):
            return float(memory_str[:-2]) / 1024
        else:
            return float(memory_str)
    except (ValueError, AttributeError):
        return None


def endpoint_only(endpoint, datetime_str, user_name, organisation,
                  internal_customer, external_customer, log_level,
                  table_name, db_obj, app_name, portfolio_id):
    # Keep datetime_str as is, store as string without conversion
    date_part = datetime_str.split()[0]  # '2025-09-03'
    time_part = datetime_str.split()[1]  # '09:01:23,714'

    data = {
        'endpoint': endpoint,
        'datetime': datetime_str,  # store raw string here
        'user_name': user_name,
        'organisation': organisation,
        'internal_customer': internal_customer,
        'external_customer': external_customer,
        'log_level': log_level,
        'app_name': app_name,
        'date': date_part,
        'time': time_part,
        'endpoints_unique': f"{user_name}_{organisation}_{date_part}",
        'portfolio_id': portfolio_id
    }

    collection = db_obj[table_name]
    query = {k: v for k, v in data.items() if v is not None}

    try:
        result = collection.update_one(query, {'$setOnInsert': data}, upsert=True)
        if result.matched_count > 0:
            print("Document already exists, skipping insert.")
            return False
        elif result.upserted_id is not None:
            print(f"Inserted new document with id {result.upserted_id}.")
            return True
        else:
            print("No changes made during upsert.")
            return False
    except Exception as e:
        print(f"MongoDB upsert error: {e}")
        return False

def insert_into_mongodb(collection_name, data_list, db_obj):
    """Kept for compatibility; inserts many documents at once."""
    try:
        collection = db_obj[collection_name]
        result = collection.insert_many(data_list)
        inserted_count = len(result.inserted_ids)
        print(f"Inserted {inserted_count} documents into collection {collection_name}.")
        return True
    except Exception as e:
        print(f"Error while inserting data into MongoDB: {e}")
        return False
