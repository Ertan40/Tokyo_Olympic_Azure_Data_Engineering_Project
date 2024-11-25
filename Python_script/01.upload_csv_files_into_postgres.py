# olympic_data_db
import pandas as pd
import psycopg2
import os


# Get all CSV files in the current working directory
def get_file_names():
    csv_files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith('.csv'):
            csv_files.append(file)
    print("CSV files found:", csv_files)
    return csv_files


# Create a dataset folder and move CSV files into it
def create_dataset_folder(csv_files, dataset_folder):
    try:
        if not os.path.exists(dataset_folder):
            os.makedirs(dataset_folder)
            print(f'Created directory: {dataset_folder}')
        else:
            print(f'Directory already exists: {dataset_folder}')
    except Exception as e:
        print(f'An error occurred while creating the directory: {e}')

    for csv in csv_files:
        try:
            src = os.path.join(os.getcwd(), csv)
            dst = os.path.join(dataset_folder, csv)
            os.rename(src, dst)
            print(f'Moved file {csv} to {dataset_folder}')
        except Exception as e:
            print(f'An error occurred while moving {csv}: {e}')


# Create a dictionary of DataFrames from CSV files in the dataset folder
def create_df(dataset_folder, csv_files):
    data_path = os.path.join(os.getcwd(), dataset_folder)
    df_dict = {}
    for file in csv_files:
        try:
            df_dict[file] = pd.read_csv(os.path.join(data_path, file))
        except UnicodeDecodeError:
            df_dict[file] = pd.read_csv(os.path.join(data_path, file), encoding="ISO-8859-1")
        print(f"Loaded file: {file}")
    return df_dict


# Clean the table name for SQL compatibility
def clean_table_name(filename):
    clean_table_name = (filename.lower().replace(' ', '').replace('%', '').replace('-', '_').replace(r'/', '_')
                        .replace('$', ''))
    table_name = '{0}'.format(clean_table_name.split('.')[0])
    return table_name


# Clean DataFrame column names and prepare column definitions for SQL
def clean_column_name(dataframe):
    dataframe.columns = [
        x.lower().replace(" ", "_").replace("-", "_").replace(r"/", "_").replace("\\", "_").replace(".", "_").replace(
            "$", "").replace("%", "") for x in dataframe.columns]

    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }

    col_str = ", ".join(
        "{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))

    return col_str, dataframe.columns


# Function to upload DataFrame data to PostgreSQL database
def upload_to_db(host, dbname, user, password, table_name, col_str, file, dataframe, dataframe_columns):
    conn_string = "host=%s dbname=%s user=%s password=%s" % (host, dbname, user, password)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print('Opened database successfully')

    try:
        cursor.execute("DROP TABLE IF EXISTS %s;" % (table_name))
        cursor.execute("CREATE TABLE %s (%s);" % (table_name, col_str))
        print('{0} was created successfully'.format(table_name))

        dataframe.to_csv(file, header=dataframe_columns, index=False, encoding='utf-8')
        with open(file, 'r') as my_file:
            print('File opened in memory')
            SQL_STATEMENT = """
            COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ','
            """ % table_name
            cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
            print('File copied to database')

        cursor.execute("GRANT SELECT ON TABLE %s TO public" % table_name)
        conn.commit()
        print('Table {0} imported to database completed'.format(table_name))
    except Exception as e:
        print(f'An error occurred: {e}')
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Define the dataset folder name
dataset_folder_name = 'dataset_folder'

# Get the list of CSV files
data = get_file_names()

# Create dataset folder and move files
create_dataset_folder(data, dataset_folder_name)

# Create DataFrame from the CSV files
create_dataframe = create_df(dataset_folder_name, data)


host = '127.0.0.1'
dbname = 'olympic_data_db'
user = 'postgres-user'
password = 'password'

for file_name, dataframe in create_dataframe.items():
    table_name = clean_table_name(file_name)
    col_str, dataframe_columns = clean_column_name(dataframe)
    upload_to_db(host, dbname, user, password, table_name, col_str, f'temp_{file_name}', dataframe, dataframe_columns)