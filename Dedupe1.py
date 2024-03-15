import os
import sys
import glob
import ntpath
import pandas as pd
import logging
import dedupe
from unidecode import unidecode
import re
import tempfile
from datetime import datetime 

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def preProcess(column):
    try:
        column = unidecode(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        return None if not column else column
    except Exception as e:
        logger.error(f"Error processing column: {column} - {e}")
        return None

def readData(filename):
    try:
        file_extension = filename.split('.')[-1].lower()
        if file_extension in ['csv']:
            df = pd.read_csv(filename)
        elif file_extension in ['xls', 'xlsx']:
            df = pd.read_excel(filename)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        data_d = {row['ID']: 
                  {str(k): preProcess(str(v)) for (k, v) in row.items()}
                  for _, row in df.iterrows() if pd.notna(row['ID'])}
        return data_d
    except Exception as e:
        logger.error(f"Error reading data from {filename}: {e}")
        return {}

def setup_temp_directory():  # New function to set up a temp directory
    temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

def setupDeduper(settings_file, training_file, data_d):
    try:
        temp_dir = setup_temp_directory()  # Use the new function to get the temp directory
        output_file = os.path.join(temp_dir, 'deduplicated_output.xlsx')  # Use temp_dir in the file path

        if os.path.exists(settings_file):
            with open(settings_file, 'rb') as f:
                return dedupe.StaticDedupe(f)
        else:
            fields = [
                {'field': 'ID', 'type': 'Exact'},
                {'field': 'ApplicationNumber', 'type': 'String'},
                {'field': 'CandidateName', 'type': 'String'},
                {'field': 'FatherName', 'type': 'String', 'has missing': True},
                {'field': 'Motherame', 'type': 'String', 'has missing': True},
            ]
            deduper = dedupe.Dedupe(fields)

            if os.path.exists(training_file):
                with open(training_file, 'rb') as f:
                    deduper.prepare_training(data_d, f)
            else:
                deduper.prepare_training(data_d)

            dedupe.console_label(deduper)
            deduper.train()

            with open(training_file, 'w') as tf:
                deduper.write_training(tf)
            with open(settings_file, 'wb') as sf:
                deduper.write_settings(sf)
            
            return deduper
    except Exception as e:
        logger.error(f"Error setting up deduper: {e}")
        return None

def deduplicateData(deduper, data_d):
    try:
        clustered_dupes = deduper.partition(data_d, 0.4)
        return clustered_dupes
    except Exception as e:
        logger.error(f"Error during deduplication: {e}")
        return []

def saveResults(clustered_dupes, data_d, output_file):
    try:
        df = pd.DataFrame.from_dict(data_d, orient='index')
        cluster_membership = {}
        for cluster_id, (records, scores) in enumerate(clustered_dupes):
            for record_id, score in zip(records, scores):
                cluster_membership[record_id] = {"Cluster ID": cluster_id, "confidence_score": score}
                df.loc[df.index == record_id, 'Cluster ID'] = cluster_id
                df.loc[df.index == record_id, 'confidence_score'] = score

        with pd.ExcelWriter(output_file) as writer:
            df.to_excel(writer, index=False)
    except Exception as e:
        logger.error(f"Error saving results to {output_file}: {e}")

def saveDuplicates(clustered_dupes, data_d, duplicates_file):
    try:
        duplicate_records = []
        for cluster_id, (records, _) in enumerate(clustered_dupes):
            if len(records) > 1:  # Only consider clusters with more than one record
                for record_id in records:
                    record = data_d[record_id]
                    record['Cluster ID'] = cluster_id  # Add cluster_id to each record
                    duplicate_records.append(record)

        duplicate_df = pd.DataFrame(duplicate_records)
        with pd.ExcelWriter(duplicates_file) as writer:
            duplicate_df.to_excel(writer, index=False)
    except Exception as e:
        logger.error(f"Error saving duplicate records to {duplicates_file}: {e}")

if __name__ == '__main__':
    # Get the latest uploaded file from the 'uploads' directory
    uploads_dir = 'uploads'
    latest_file = max(glob.glob(os.path.join(uploads_dir, '*')), key=os.path.getctime)

    input_file = latest_file
    output_file = os.path.join(setup_temp_directory(), 'deduplicated_output.xlsx')

    settings_file = 'learned_settings'
    training_file = 'training.json'
    duplicates_file = os.path.join(setup_temp_directory(), 'temp_out', 'duplicates_output.xlsx')


    data_d = readData(input_file)
    if data_d:
        deduper = setupDeduper(settings_file, training_file, data_d)
        if deduper:
            clustered_dupes = deduplicateData(deduper, data_d)
            if clustered_dupes:
                saveResults(clustered_dupes, data_d, output_file)
                saveDuplicates(clustered_dupes, data_d, duplicates_file)

                duplicate_count = 0
                for cluster in clustered_dupes:
                    if len(cluster[0]) > 1:
                        duplicate_count += len(cluster[0])
                print(f"Total duplicate records: {duplicate_count}")
                print(f"Total duplicate groups: {len(clustered_dupes)}")
            else:
                logger.error("No clustered duplicates found.")
        else:
            logger.error("Failed to set up deduper.")
    else:
        logger.error("Failed to read data.")
