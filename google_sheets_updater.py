"""
Google Sheets Database Updater

This script updates the database with data from a Google Sheets document.
It reads data from two tabs in the Google Sheet:
1. "Classes" - Updates the ClassClarify table with group_name and purpose values
2. "Characteristics" - Updates the CharacteristicClarify table with characteristics and priority values

Usage:
    python google_sheets_updater.py

Requirements:
    - pandas
    - requests
    - sqlalchemy

The Google Sheet must be publicly accessible for reading.
The script uses the CSV export feature of Google Sheets to download the data.

Expected columns in the "Classes" tab:
    - class_rusname (must match the class_rusname in the database)
    - group_name
    - purpose

Expected columns in the "Characteristics" tab:
    - characteristic_good (must match the characteristic_good in the database)
    - characteristics
    - priority (must be an integer)
"""

import os
import pandas as pd
import aiohttp
import asyncio
import logging
from io import StringIO
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update, select
from db import AsyncSessionLocal
from models import ClassClarify, CharacteristicClarify
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO to DEBUG to show encoding debug logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Google Sheets URL and IDs
SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
CLASSES_GID = os.getenv("GOOGLE_CLASSES_GID")  # GID for the Classes tab
CHARACTERISTICS_GID = os.getenv("GOOGLE_CHARACTERISTICS_GID")  # GID for the Characteristics tab (default is 0 for the first tab)

# Note: To find the GID of a tab, look at the URL when you have the tab open:
# https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit#gid=TAB_GID

async def get_sheet_data(gid, tab_name=""):
    """
    Get data from a Google Sheet tab using the CSV export feature.

    Args:
        gid (str): The GID of the sheet tab
        tab_name (str, optional): The name of the tab for logging purposes

    Returns:
        pandas.DataFrame: The sheet data as a DataFrame or None if an error occurs
    """
    tab_info = f" for {tab_name} tab" if tab_name else ""

    try:
        # Construct the CSV export URL
        csv_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid={gid}"
        logger.info(f"Downloading data{tab_info} from {csv_url}")

        # Download the CSV data using aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(csv_url, timeout=30) as response:  # Add timeout to prevent hanging
                # Check for HTTP errors
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"HTTP error {response.status}{tab_info}: {error_text[:500]}")

                    # Try alternative URL format as fallback
                    alt_csv_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&gid={gid}"
                    logger.info(f"Trying alternative URL{tab_info}: {alt_csv_url}")

                    try:
                        async with session.get(alt_csv_url, timeout=30) as alt_response:
                            if alt_response.status == 200:
                                alt_text = await alt_response.text()
                                data = StringIO(alt_text)
                                df = pd.read_csv(data, encoding='utf-8')
                                logger.info(f"Successfully downloaded data{tab_info} with alternative URL: {len(df)} rows")

                                # Log a sample of the data to verify encoding
                                if not df.empty:
                                    logger.debug(f"Sample data from {tab_name} (alternative URL, first row):\n{df.iloc[0].to_dict()}")

                                return df
                            else:
                                logger.error(f"Alternative URL also failed with HTTP error {alt_response.status}{tab_info}")
                    except Exception as alt_e:
                        logger.error(f"Error with alternative URL{tab_info}: {str(alt_e)}")

                    return None

                # Get the response text
                response_text = await response.text()

                # Convert the CSV data to a DataFrame
                data = StringIO(response_text)
                df = pd.read_csv(data, encoding='utf-8')

                logger.info(f"Successfully downloaded data{tab_info} with {len(df)} rows")

                # Log a sample of the data to verify encoding
                if not df.empty:
                    logger.debug(f"Sample data from {tab_name} (first row):\n{df.iloc[0].to_dict()}")

                return df
    except asyncio.TimeoutError:
        logger.error(f"Request timed out{tab_info}. Check your internet connection or try again later.")
        return None
    except aiohttp.ClientConnectionError:
        logger.error(f"Connection error{tab_info}. Check your internet connection.")
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"The downloaded file{tab_info} is empty or not a valid CSV.")
        return None
    except Exception as e:
        logger.error(f"Error getting sheet data{tab_info}: {str(e)}", exc_info=True)
        return None

async def update_classes(session: AsyncSession):
    """
    Update the ClassClarify table with data from the Classes tab in Google Sheets.

    For each class_rusname in the database, find the corresponding row in the Google Sheet
    and update the group_name and purpose columns.
    """
    try:
        logger.info("Updating Classes...")

        # Get the Classes data from Google Sheets
        df = await get_sheet_data(CLASSES_GID, "Classes")
        if df is None:
            logger.error("Failed to get Classes data from Google Sheets.")
            return

        # Validate that the required columns exist
        required_columns = ['class_rusname']
        optional_columns = ['group_name', 'purpose']

        # Convert DataFrame column names to lowercase for case-insensitive comparison
        df.columns = [col.lower() for col in df.columns]
        logger.info(f"Classes sheet columns (lowercase): {list(df.columns)}")

        # Convert required and optional columns to lowercase
        required_columns_lower = [col.lower() for col in required_columns]
        optional_columns_lower = [col.lower() for col in optional_columns]

        missing_required = [col for col in required_columns if col.lower() not in df.columns]
        if missing_required:
            logger.error(f"Missing required columns in Classes sheet: {missing_required}")
            return

        missing_optional = [col for col in optional_columns if col.lower() not in df.columns]
        if missing_optional:
            logger.warning(f"Missing optional columns in Classes sheet: {missing_optional}")

        # Get all class_rusname values from the database
        result = await session.execute(select(ClassClarify))
        db_classes = result.scalars().all()
        logger.info(f"Found {len(db_classes)} classes in the database")

        # Update count
        updated_count = 0

        # For each class in the database
        for db_class in db_classes:
            try:
                # Find the corresponding row in the Google Sheet (using lowercase column name)
                matching_rows = df[df['class_rusname'] == db_class.class_rusname]

                if not matching_rows.empty:
                    # Get the first matching row
                    row = matching_rows.iloc[0]

                    # Store old values for logging
                    old_group_name = db_class.group_name
                    old_purpose = db_class.purpose

                    # Update the database (using lowercase column names)
                    if 'group_name' in row:
                        # Check if the value is NaN and set to None if it is
                        if pd.isna(row['group_name']):
                            db_class.group_name = None
                        else:
                            db_class.group_name = row['group_name']
                        if old_group_name != db_class.group_name:
                            logger.debug(f"Updated group_name for {db_class.class_rusname}: '{old_group_name}' -> '{db_class.group_name}'")

                    if 'purpose' in row:
                        # Check if the value is NaN and set to None if it is
                        if pd.isna(row['purpose']):
                            db_class.purpose = None
                        else:
                            db_class.purpose = row['purpose']
                        if old_purpose != db_class.purpose:
                            logger.debug(f"Updated purpose for {db_class.class_rusname}: '{old_purpose}' -> '{db_class.purpose}'")

                    updated_count += 1
            except Exception as e:
                logger.warning(f"Error processing class {db_class.class_rusname}: {str(e)}")

        # Commit the changes
        await session.commit()

        logger.info(f"Updated {updated_count} classes out of {len(db_classes)}")

    except Exception as e:
        await session.rollback()
        logger.error(f"Error updating classes: {str(e)}", exc_info=True)

async def update_characteristics(session: AsyncSession):
    """
    Update the CharacteristicClarify table with data from the Characteristics tab in Google Sheets.

    For each characteristic_good in the database, find the corresponding row in the Google Sheet
    and update the characteristics and priority columns.
    """
    try:
        logger.info("Updating Characteristics...")

        # Get the Characteristics data from Google Sheets
        df = await get_sheet_data(CHARACTERISTICS_GID, "Characteristics")
        if df is None:
            logger.error("Failed to get Characteristics data from Google Sheets.")
            return

        # Validate that the required columns exist
        required_columns = ['characteristic_good']
        optional_columns = ['characteristic', 'priority']

        # Convert DataFrame column names to lowercase for case-insensitive comparison
        df.columns = [col.lower() for col in df.columns]
        logger.info(f"Characteristics sheet columns (lowercase): {list(df.columns)}")

        # Convert required and optional columns to lowercase
        required_columns_lower = [col.lower() for col in required_columns]
        optional_columns_lower = [col.lower() for col in optional_columns]

        missing_required = [col for col in required_columns if col.lower() not in df.columns]
        if missing_required:
            logger.error(f"Missing required columns in Characteristics sheet: {missing_required}")
            return

        missing_optional = [col for col in optional_columns if col.lower() not in df.columns]
        if missing_optional:
            logger.warning(f"Missing optional columns in Characteristics sheet: {missing_optional}")

        # Get all characteristic_good values from the database
        result = await session.execute(select(CharacteristicClarify))
        db_characteristics = result.scalars().all()
        logger.info(f"Found {len(db_characteristics)} characteristics in the database")

        # Create a dictionary of existing characteristic values to check for duplicates
        existing_characteristics = {}
        for char in db_characteristics:
            if char.characteristic:
                existing_characteristics[char.characteristic] = char.id

        logger.debug(f"Found {len(existing_characteristics)} unique characteristic values in the database")

        # Update count
        updated_count = 0
        skipped_count = 0

        # For each characteristic in the database
        for db_char in db_characteristics:
            try:
                # Find the corresponding row in the Google Sheet (using lowercase column name)
                matching_rows = df[df['characteristic'] == db_char.characteristic]

                if not matching_rows.empty:
                    # Get the first matching row
                    row = matching_rows.iloc[0]

                    # Store old values for logging
                    old_characteristic = db_char.characteristic
                    old_priority = db_char.priority

                    # Update the database (using lowercase column names)
                    if 'characteristic_good' in row:
                        # Check if the value is NaN and set to None if it is
                        if pd.isna(row['characteristic_good']):
                            db_char.characteristic_good = None
                        else:
                            new_characteristic = row['characteristic_good']

                            # # Check if this characteristic value already exists in another row
                            # if (new_characteristic in existing_characteristics and
                            #     existing_characteristics[new_characteristic] != db_char.id and
                            #     new_characteristic != old_characteristic):
                            #     logger.warning(
                            #         f"Skipping update for {db_char.characteristic_good} (ID: {db_char.id}): "
                            #         f"characteristic value '{new_characteristic}' already exists in row with ID: "
                            #         f"{existing_characteristics[new_characteristic]}"
                            #     )
                            #     skipped_count += 1
                            #     continue

                            # Update the characteristic value
                            db_char.characteristic_good = new_characteristic

                            # Update the existing_characteristics dictionary
                            if old_characteristic:
                                if old_characteristic in existing_characteristics and existing_characteristics[old_characteristic] == db_char.id:
                                    del existing_characteristics[old_characteristic]
                            if new_characteristic:
                                existing_characteristics[new_characteristic] = db_char.id

                            # if old_characteristic != db_char.characteristic:
                            #     logger.debug(f"Updated characteristic for {db_char.characteristic_good}: '{old_characteristic}' -> '{db_char.characteristic}'")

                    # Handle priority field
                    if 'priority' in row:
                        if pd.isna(row['priority']):
                            # Set priority to None if the value is NaN
                            db_char.priority = None
                            if old_priority != db_char.priority:
                                logger.debug(f"Updated priority for {db_char.characteristic_good}: {old_priority} -> None")
                        else:
                            # Convert priority to integer if it's not NaN
                            try:
                                db_char.priority = int(row['priority'])
                                if old_priority != db_char.priority:
                                    logger.debug(f"Updated priority for {db_char.characteristic_good}: {old_priority} -> {db_char.priority}")
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid priority value for {db_char.characteristic_good}: {row['priority']}")

                    updated_count += 1
            except Exception as e:
                logger.warning(f"Error processing characteristic {db_char.characteristic_good}: {str(e)}")

        # Commit the changes
        await session.commit()

        logger.info(f"Updated {updated_count} characteristics out of {len(db_characteristics)}, skipped {skipped_count} due to unique constraint")

    except Exception as e:
        await session.rollback()
        logger.error(f"Error updating characteristics: {str(e)}", exc_info=True)

async def main():
    """
    Main function to update the database with data from Google Sheets.
    """
    logger.info("Starting database update from Google Sheets...")

    # Create a database session
    session = AsyncSessionLocal()

    try:
        # Update classes
        await update_classes(session)

        # Update characteristics
        await update_characteristics(session)

        logger.info("Database update completed successfully.")
    except Exception as e:
        logger.error(f"Error during database update: {str(e)}", exc_info=True)
    finally:
        await session.close()
        logger.info("Database session closed.")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
