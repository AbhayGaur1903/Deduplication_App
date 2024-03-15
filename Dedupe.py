# import os
# import pandas as pd
# import logging
# import dedupe
# import asyncio
# from unidecode import unidecode
# import re
# from sqlalchemy import create_engine
# import psycopg2
# from urllib.parse import quote_plus

# # Logging errors
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# def create_db_engine(dbtype):
#     try:
#         if dbtype.lower() in ("postgres", "p"):
#             username = input("Enter PostgreSQL username: ")
#             password = input("Enter PostgreSQL password: ")
#             hostname = input("Enter PostgreSQL hostname: ")
#             port = input("Enter PostgreSQL port (default is 5432): ") or "5432"
#             dbname = input("Enter PostgreSQL database name: ")

#             # Encode special characters in the password
#             encoded_password = quote_plus(password)
#             engine = create_engine(f"postgresql://postgres:{encoded_password}@{hostname}:{port}/{dbname}")

#         elif dbtype.lower() in ("mysql", "m"):
#             username = input("Enter MySQL username: ")
#             password = input("Enter MySQL password: ")
#             hostname = input("Enter MySQL hostname: ")
#             port = input("Enter MySQL port (default is 3306): ") or "3306"
#             dbname = input("Enter MySQL database name: ")

#             # Encode special characters in the password
#             encoded_password = quote_plus(password)

#             logger.log(encoded_password)

#             engine = create_engine(f"mysql+pymysql://{username}:{encoded_password}@{hostname}:{port}/{dbname}")
#         else:
#             raise ValueError("Unsupported database type. Supported types are 'postgres' and 'mysql'")
        
#         return engine
#     except Exception as e:
#         logger.error(f"Database connection failed: {e}")
#         return None


# def pre_process(column):
#     try:
#         if column is not None:
#             column = unidecode(column)
#             column = re.sub('  +', ' ', column)
#             column = re.sub('\n', ' ', column)
#             column = column.strip().strip('"').strip("'").lower().strip()
#             return column
#         else:
#             return None
#     except Exception as e:
#         logger.error(f"Error processing column: {column} - {e}")
#         return None

# def read_data(table_name=None, field_mapping=None):
#     try:
#         # Create a SQLAlchemy engine
#         dbtype = input("Enter the database type (postgres(p)/mysql(m)): ")
#         engine = create_db_engine(dbtype)
#         if not engine:
#             return {}

#         # Fetch data from the table into a DataFrame
#         query = f"SELECT * FROM \"{table_name}\""
#         df = pd.read_sql_query(query, engine)

#         # Close the SQLAlchemy engine
#         engine.dispose()

#         # Rename columns according to the provided mapping
#         df = df.rename(columns=field_mapping)

#         # Convert DataFrame to dictionary
#         data_d = df.to_dict(orient='index')
        
#         return data_d
#     except Exception as e:
#         logger.error(f"Error reading data from database: {e}")
#         return {}

# def setup_deduper(data_d, field_mapping):
#     try:
#         # Path to learned settings and training file
#         settings_file = 'learned_settings'
#         training_file = 'training.json'
#         confidence_threshold = 0.5

#         if os.path.exists(settings_file):
#             with open(settings_file, 'rb') as f:
#                 return dedupe.StaticDedupe(f)
        
#     except Exception as e:
#         logger.error(f"Error setting up deduper: {e}")
#         return None

# def deduplicate_data(deduper, data_d):
#     try:
#         clustered_dupes = deduper.partition(data_d,confidence_threshold)
#         return clustered_dupes
#     except Exception as e:
#         logger.error(f"Error during deduplication: {e}")
#         return []

# def save_results(clustered_dupes, data_d, output_file):
#     try:
#         duplicate_records = [
#             {**data_d[record_id], 'cluster_id': cluster_id, 'confidence_score': score}
#             for cluster_id, (records, scores) in enumerate(clustered_dupes)
#             if len(records) > 1 for record_id, score in zip(records, scores)
#         ]

        

#         duplicate_df = pd.DataFrame(duplicate_records)
#         duplicate_df.to_excel(output_file, index=False)  # Corrected syntax
#         logger.info(f"Duplicate records saved to Excel file: {output_file}")
#     except Exception as e:
#         logger.error(f"Error saving results to {output_file}: {e}")

# if __name__ == '__main__':
#     try:
#         # Database fetching and deduplication steps
#         table_name = input("Enter the name of the table: ")
#         field_mapping = {
#             input("Enter the column name for CandidateName: "): "CandidateName",
#             input("Enter the column name for FatherName: "): "FatherName",
#             input("Enter the column name for MotherName: "): "MotherName",
#             # Add more mappings as needed
#         }

#         # Define output file name
#         duplicates_file = 'duplicates_output.xlsx'

#         data_d = read_data(table_name, field_mapping)
#         if data_d:
#             print(data_d)
#             deduper = setup_deduper(data_d, field_mapping)
#             if deduper:
#                 clustered_dupes = deduplicate_data(deduper, data_d)
#                 if clustered_dupes:
#                     save_results(clustered_dupes, data_d, duplicates_file)
#                 else:
#                     logger.error("No clustered duplicates found.")
#             else:
#                 logger.error("Failed to set up deduper.")
#         else:
#             logger.error("Failed to read data.")
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
   
# valid Script
# import os
# import pandas as pd
# import logging
# import dedupe
# import asyncio
# from unidecode import unidecode
# import re
# from sqlalchemy import create_engine
# import psycopg2
# from urllib.parse import quote_plus
# import argparse

# # Logging errors
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def create_db_engine(dbtype, username, password, hostname, port, dbname):
#     try:
#         if dbtype.lower() in ("postgres", "p"):
#             encoded_password = quote_plus(password)
#             engine = create_engine(f"postgresql://{username}:{encoded_password}@{hostname}:{port}/{dbname}")

#         elif dbtype.lower() in ("mysql", "m"):
#             encoded_password = quote_plus(password)
#             engine = create_engine(f"mysql+pymysql://{username}:{encoded_password}@{hostname}:{port}/{dbname}")
#         else:
#             raise ValueError("Unsupported database type. Supported types are 'postgres' and 'mysql'")
        
#         return engine
#     except Exception as e:
#         logger.error(f"Database connection failed: {e}")
#         return None

# def pre_process(column):
#     try:
#         if column is not None:
#             column = unidecode(column)
#             column = re.sub('  +', ' ', column)
#             column = re.sub('\n', ' ', column)
#             column = column.strip().strip('"').strip("'").lower().strip()
#             return column
#         else:
#             return None
#     except Exception as e:
#         logger.error(f"Error processing column: {column} - {e}")
#         return None

# def read_data(engine, table_name, field_mapping):
#     try:
#         query = f"SELECT * FROM \"{table_name}\""
#         df = pd.read_sql_query(query, engine)
#         engine.dispose()

#         df = df.rename(columns=field_mapping)
#         data_d = df.to_dict(orient='index')
        
#         return data_d
#     except Exception as e:
#         logger.error(f"Error reading data from database: {e}")
#         return {}

# def setup_deduper(data_d, field_mapping):
#     try:
#         settings_file = 'learned_settings'
#         training_file = 'training.json'
#         confidence_threshold = 0.5

#         if os.path.exists(settings_file):
#             with open(settings_file, 'rb') as f:
#                 return dedupe.StaticDedupe(f)
#     except Exception as e:
#         logger.error(f"Error setting up deduper: {e}")
#         return None

# def deduplicate_data(deduper, data_d):
#     try:
#         clustered_dupes = deduper.partition(data_d, confidence_threshold)
#         return clustered_dupes
#     except Exception as e:
#         logger.error(f"Error during deduplication: {e}")
#         return []

# def save_results(clustered_dupes, data_d, output_file):
#     try:
#         duplicate_records = [
#             {**data_d[record_id], 'cluster_id': cluster_id, 'confidence_score': score}
#             for cluster_id, (records, scores) in enumerate(clustered_dupes)
#             if len(records) > 1 for record_id, score in zip(records, scores)
#         ]

#         duplicate_df = pd.DataFrame(duplicate_records)
#         duplicate_df.to_excel(output_file, index=False)
#         logger.info(f"Duplicate records saved to Excel file: {output_file}")
#     except Exception as e:
#         logger.error(f"Error saving results to {output_file}: {e}")

# if __name__ == '__main__':
#     try:
#         table_name = input("Enter the name of the table: ")
#         field_mapping = {
#             input("Enter the column name for CandidateName: "): "CandidateName",
#             input("Enter the column name for FatherName: "): "FatherName",
#             input("Enter the column name for MotherName: "): "MotherName",
#             # Add more mappings as needed
#         }

#         duplicates_file = 'duplicates_output.xlsx'

#         # Accept user input for database details
#         dbtype = input("Enter the database type (postgres(p)/mysql(m)): ")
#         username = input(f"Enter {dbtype} username: ")
#         password = input(f"Enter {dbtype} password: ")
#         hostname = input(f"Enter {dbtype} hostname: ")
#         port = input(f"Enter {dbtype} port: ")
#         dbname = input(f"Enter {dbtype} database name: ")

#         engine = create_db_engine(dbtype, username, password, hostname, port, dbname)
#         if engine:
#             data_d = read_data(engine, table_name, field_mapping)
#             if data_d:
#                 deduper = setup_deduper(data_d, field_mapping)
#                 if deduper:
#                     clustered_dupes = deduplicate_data(deduper, data_d)
#                     if clustered_dupes:
#                         save_results(clustered_dupes, data_d, duplicates_file)
#                     else:
#                         logger.error("No clustered duplicates found.")
#                 else:
#                     logger.error("Failed to set up deduper.")
#             else:
#                 logger.error("Failed to read data.")
#         else:
#             logger.error("Failed to connect to the database.")
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         traceback.print_exc() 



# testing
# import os
# import pandas as pd
# import logging
# import dedupe
# from unidecode import unidecode
# import re
# from sqlalchemy import create_engine
# from urllib.parse import quote_plus
# import argparse

# # Logging configuration
# logging.basicConfig(filename='dedupe.log', level=logging.INFO)
# logger = logging.getLogger(__name__)

# def create_db_engine(dbtype, dbname, user, password, host, port):
#     try:
#         if dbtype.lower() in ("postgres", "p"):
#             encoded_password = quote_plus(password)
#             engine = create_engine(f"postgresql://{user}:{encoded_password}@{host}:{port}/{dbname}")
#         elif dbtype.lower() in ("mysql", "m"):
#             encoded_password = quote_plus(password)
#             engine = create_engine(f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{dbname}")
#         else:
#             raise ValueError("Unsupported database type. Supported types are 'postgres' and 'mysql'")
#         return engine
#     except Exception as e:
#         logger.error(f"Database connection failed: {e}")
#         return None

# def pre_process(column):
#     try:
#         if column is not None:
#             column = unidecode(column)
#             column = re.sub('  +', ' ', column)
#             column = re.sub('\n', ' ', column)
#             column = column.strip().strip('"').strip("'").lower().strip()
#             return column
#         else:
#             return None
#     except Exception as e:
#         logger.error(f"Error processing column: {column} - {e}")
#         return None

# def read_data(engine, table_name, field_mapping):
#     try:
#         query = f"SELECT * FROM \"{table_name}\""
#         df = pd.read_sql_query(query, engine)
#         engine.dispose()
#         df = df.rename(columns=field_mapping)
#         data_d = df.to_dict(orient='index')
#         return data_d
#     except Exception as e:
#         logger.error(f"Error reading data from database: {e}")
#         return {}

# def setup_deduper(data_d, field_mapping, confidence):
#     try:
#         settings_file = 'learned_settings'
#         training_file = 'training.json'

#         if os.path.exists(settings_file):
#             with open(settings_file, 'rb') as f:
#                 deduper = dedupe.StaticDedupe(f)
#                 deduper.threshold = confidence
#                 return deduper
#     except Exception as e:
#         logger.error(f"Error setting up deduper: {e}")
#         return None

# def deduplicate_data(deduper, data_d, confidence):
#     try:
#         clustered_dupes = deduper.partition(data_d, confidence)
#         return clustered_dupes
#     except Exception as e:
#         logger.error(f"Error during deduplication: {e}")
#         return []

# def save_results(clustered_dupes, data_d, output_file):
#     try:
#         duplicate_records = [
#             {**data_d[record_id], 'cluster_id': cluster_id, 'confidence_score': score}
#             for cluster_id, (records, scores) in enumerate(clustered_dupes)
#             if len(records) > 1 for record_id, score in zip(records, scores)
#         ]

#         duplicate_df = pd.DataFrame(duplicate_records)
#         duplicate_df.to_csv(output_file, index=False)
#         logger.info(f"Duplicate records saved to CSV file: {output_file}")
#     except Exception as e:
#         logger.error(f"Error saving results to {output_file}: {e}")


# def run_dedupe(dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername):
#     try:
#         logger.info(f"Received parameters: {dbtype}, {dbname}, {user}, {password}, {host}, {port}, {confidence}, {tablename}, {candidatename}, {fathername}, {mothername}")

#         field_mapping = {
#             candidatename: "CandidateName",
#             fathername: "FatherName",
#             mothername: "MotherName",
#         }

#         engine = create_db_engine(dbtype, dbname, user, password, host, port)
#         if engine:
#             data_d = read_data(engine, tablename, field_mapping)
#             if data_d:
#                 deduper = setup_deduper(data_d, field_mapping, confidence)
#                 if deduper:
#                     clustered_dupes = deduplicate_data(deduper, data_d, confidence)
#                     if clustered_dupes:
#                         save_results(clustered_dupes, data_d, 'duplicates_output.csv')
#                     else:
#                         logger.error("No clustered duplicates found.")
#                 else:
#                     logger.error("Failed to set up deduper.")
#             else:
#                 logger.error("Failed to read data.")
#         else:
#             logger.error("Failed to connect to the database.")
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         exit(2)  # Exit with non-zero status to indicate error


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Deduplication script for database.')
#     parser.add_argument('dbtype', help='Database type (postgres/mysql)')
#     parser.add_argument('dbname', help='Database name')
#     parser.add_argument('user', help='Database username')
#     parser.add_argument('password', help='Database password')
#     parser.add_argument('host', help='Database hostname')
#     parser.add_argument('port', help='Database port')
#     parser.add_argument('confidence', help='Confidence Threshold')
#     parser.add_argument('tablename', help='Table name')
#     parser.add_argument('candidatename', help='Column name for CandidateName')
#     parser.add_argument('fathername', help='Column name for FatherName')
#     parser.add_argument('mothername', help='Column name for MotherName')
#     args = parser.parse_args()

#     run_dedupe(args.dbtype, args.dbname, args.user, args.password, args.host, args.port, args.confidence, args.tablename, args.candidatename, args.fathername, args.mothername)



# log:
import os
import pandas as pd
import logging
import dedupe
from unidecode import unidecode
import re
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import argparse

# Logging configuration
logging.basicConfig(filename='dedupe.log', level=logging.INFO)
logger = logging.getLogger(__name__)

def create_db_engine(dbtype, dbname, user, password, host, port):
    try:
        if dbtype.lower() in ("postgres", "p"):
            encoded_password = quote_plus(password)
            engine = create_engine(f"postgresql://{user}:{encoded_password}@{host}:{port}/{dbname}")
        elif dbtype.lower() in ("mysql", "m"):
            encoded_password = quote_plus(password)
            engine = create_engine(f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{dbname}")
        else:
            raise ValueError("Unsupported database type. Supported types are 'postgres' and 'mysql'")
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def pre_process(column):
    try:
        if column is not None:
            column = unidecode(column)
            column = re.sub('  +', ' ', column)
            column = re.sub('\n', ' ', column)
            column = column.strip().strip('"').strip("'").lower().strip()
            return column
        else:
            return None
    except Exception as e:
        logger.error(f"Error processing column: {column} - {e}")
        return None

def read_data(engine, table_name, field_mapping):
    try:
        query = f"SELECT * FROM \"{table_name}\""
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        df = df.rename(columns=field_mapping)
        data_d = df.to_dict(orient='index')
        return data_d
    except Exception as e:
        logger.error(f"Error reading data from database: {e}")
        return {}

def setup_deduper(data_d, field_mapping, confidence):
    try:
        settings_file = 'learned_settings'
        training_file = 'training.json'

        if os.path.exists(settings_file):
            with open(settings_file, 'rb') as f:
                deduper = dedupe.StaticDedupe(f)
                deduper.threshold = confidence
                return deduper
    except Exception as e:
        logger.error(f"Error setting up deduper: {e}")
        return None

def deduplicate_data(deduper, data_d, confidence):
    try:
        problematic_records = []
        for record_id, record in data_d.items():
            try:
                deduper.match(record)
            except Exception as e:
                logger.error(f"Error matching record {record_id}: {e}")
                problematic_records.append((record_id, record))

        if problematic_records:
            logger.error("Problematic records:")
            for record_id, record in problematic_records:
                logger.error(f"Record ID: {record_id}, Record: {record}")

        clustered_dupes = deduper.partition(data_d, confidence)
        return clustered_dupes
    except Exception as e:
        logger.error(f"Error during deduplication: {e}")
        return []


def save_results(clustered_dupes, data_d, output_file):
    try:
        duplicate_records = [
            {**data_d[record_id], 'cluster_id': cluster_id, 'confidence_score': score}
            for cluster_id, (records, scores) in enumerate(clustered_dupes)
            if len(records) > 1 for record_id, score in zip(records, scores)
        ]

        duplicate_df = pd.DataFrame(duplicate_records)
        duplicate_df.to_csv(output_file, index=False)
        logger.info(f"Duplicate records saved to CSV file: {output_file}")
    except Exception as e:
        logger.error(f"Error saving results to {output_file}: {e}")


def run_dedupe(dbtype, dbname, user, password, host, port, confidence, tablename, candidatename, fathername, mothername):
    try:
        logger.info(f"Received parameters: {dbtype}, {dbname}, {user}, {password}, {host}, {port}, {confidence}, {tablename}, {candidatename}, {fathername}, {mothername}")

        field_mapping = {
            candidatename: "CandidateName",
            fathername: "FatherName",
            mothername: "MotherName",
        }

        engine = create_db_engine(dbtype, dbname, user, password, host, port)
        if engine:
            data_d = read_data(engine, tablename, field_mapping)
            if data_d:
                deduper = setup_deduper(data_d, field_mapping, confidence)
                if deduper:
                    clustered_dupes = deduplicate_data(deduper, data_d, confidence)
                    if clustered_dupes:
                        save_results(clustered_dupes, data_d, 'duplicates_output.csv')
                    else:
                        logger.error("No clustered duplicates found.")
                else:
                    logger.error("Failed to set up deduper.")
            else:
                logger.error("Failed to read data.")
        else:
            logger.error("Failed to connect to the database.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        exit(2)  # Exit with non-zero status to indicate error


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deduplication script for database.')
    parser.add_argument('dbtype', help='Database type (postgres/mysql)')
    parser.add_argument('dbname', help='Database name')
    parser.add_argument('user', help='Database username')
    parser.add_argument('password', help='Database password')
    parser.add_argument('host', help='Database hostname')
    parser.add_argument('port', help='Database port')
    parser.add_argument('confidence',type = float, help='Confidence Threshold')
    parser.add_argument('tablename', help='Table name')
    parser.add_argument('candidatename', help='Column name for CandidateName')
    parser.add_argument('fathername', help='Column name for FatherName')
    parser.add_argument('mothername', help='Column name for MotherName')
    args = parser.parse_args()

    

    run_dedupe(args.dbtype, args.dbname, args.user, args.password, args.host, args.port, args.confidence, args.tablename, args.candidatename, args.fathername, args.mothername)
