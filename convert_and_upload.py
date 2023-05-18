import os
import re
import shutil
import subprocess
from argparse import ArgumentParser
from collections import defaultdict

import pandas as pd

from xlsx2csv import convert_recursive


def sort_dtypes(dtype: object) -> int:
    if dtype.name == 'Float64' or dtype.name == 'float64':
        return 1
    elif dtype.name == 'string':
        return 2
    elif dtype.name == 'object':
        return 3
    else:
        return 0


def convert_and_upload(bq_dest: str, local_source: str = None, c_storage_source: str = None,
                       recover: bool = False, files_to_combine: int = 10) -> None:
    """Converts all xlsx files in a local or gcloud directory to csv and uploads them to a BigQuery table

    Args:
        bq_dest: The BigQuery table to upload to
        local_source: The local directory to search for xlsx files recursively
        c_storage_source: The Cloud Storage directory to search for xlsx files recursively
        recover: Whether to recover from a previous run, loading from the temp directory specified in local_source
        files_to_combine: The number of files to combine into a single csv file before uploading to BigQuery

    Returns:
        None

    Raises:
        OSError: Raised when no xlsx files are found in the source directory
        RuntimeError: Raised when no source is specified
    """
    bq_load_options = ("--allow_quoted_newlines=true --schema_update_option=ALLOW_FIELD_ADDITION "
                       "--schema_update_option=ALLOW_FIELD_RELAXATION --allow_jagged_rows=true "
                       "--skip_leading_rows=1 --source_format=CSV")

    date_from_filename_pattern = r"(\d{4})(\d{2})(\d{2})"

    if not recover:
        output_dir = find_temp_dir()

        if c_storage_source:
            input_source = find_temp_dir()
            os.system(f"gcloud storage cp -r {c_storage_source} {input_source}")
        elif local_source:
            input_source = local_source
        else:
            raise RuntimeError("No source specified")

        if len(os.listdir(input_source)) == 0:
            raise OSError(f"No xlsx files found at {c_storage_source}")

        convert_recursive(input_source, 1, output_dir, {"escape_strings": True})
    else:
        output_dir = local_source

    combined_dir = find_temp_dir()

    data_types = {}
    csv_schemas = {}
    pandas_dtypes = [
        'datetime64[ns, <tz>]',
        'boolean',
        'Int64',
        'Float64',
        'string',
    ]
    pandas_to_bq_types = {
        'datetime64[ns, <tz>]': 'DATETIME',
        'datetime64[ns]': 'DATETIME',
        'boolean': 'BOOL',
        'bool': 'BOOL',
        'Int64': 'INT64',
        'int64': 'INT64',
        'Float64': 'FLOAT64',
        'float64': 'FLOAT64',
        'string': 'STRING',
        'object': 'STRING',
    }

    dtype_combinations_to_bq_type = {
        frozenset({'bool', 'float64'}): 'string',
        frozenset({'boolean', 'float64'}): 'string',
    }

    file_counts = defaultdict(int)

    schema_ids = {}
    next_id = 0

    total_files = len(list(os.scandir(output_dir)))
    with os.scandir(output_dir) as it:
        for i, file in enumerate(it, start=1):
            print(f'{i}/{total_files}')
            match = re.search(date_from_filename_pattern, file.path)
            year = match.group(1)
            month = match.group(2)
            day = match.group(3)
            date = f"{year}-{month}-{day}"

            df = pd.read_csv(file.path, low_memory=False)
            df.columns = [re.sub(r'[\\/ #\-]', '_', col) for col in df.columns]
            df.columns = [re.sub(r'[?\n\r]', '', col) for col in df.columns]
            df['date_exported'] = date

            df = df[sorted(df)]

            for column, _ in [x for x in df.dtypes.items() if x[1] == 'object']:
                for dtype in pandas_dtypes:
                    try:
                        df[column] = df[column].astype(dtype)
                        break
                    except (ValueError, TypeError):
                        pass
            schema_key = frozenset(df.dtypes.items())
            try:
                schema_id = str(schema_ids[schema_key])
            except KeyError:
                schema_ids[schema_key] = next_id
                schema_id = str(next_id)
                next_id += 1
            file_index = file_counts[schema_id] // files_to_combine
            file_counts[schema_id] += 1
            output_file = f"{combined_dir}/{schema_id}_{file_index}.csv"

            if os.path.isfile(output_file):
                df.to_csv(output_file, mode='a', header=False, index=False)
            else:
                df.to_csv(output_file, mode='w', index=False)

    schema_ids = {v: k for k, v in schema_ids.items()}
    for _, i in schema_ids.items():
        for j in i:
            column, dtype = j
            if column not in data_types:
                data_types[column] = set()
            data_types[column].add(dtype)
    bq_table_schema = {}
    for column, dtype_set in data_types.items():
        dtype_names = frozenset(dtype.name for dtype in dtype_set)
        if dtype_names in dtype_combinations_to_bq_type:
            base_pandas_type_name = dtype_combinations_to_bq_type[dtype_names]
        else:
            base_pandas_type = max(dtype_set, key=sort_dtypes)
            base_pandas_type_name = base_pandas_type.name
        bq_type = pandas_to_bq_types[base_pandas_type_name]
        bq_table_schema[column] = bq_type
    for file_name, _ in schema_ids.items():
        csv_schemas[file_name] = {column: bq_table_schema[column] for column, _ in schema_ids[file_name]}

    schema_string = ','.join([f'{column}:{bq_type}' for column, bq_type in bq_table_schema.items()])

    os.system(f"bq mk --schema={schema_string} -t {bq_dest}")

    os.system("gcloud storage rm gs://skynamo_history/temp/**")

    os.system(f"gcloud storage cp {combined_dir}/*.csv gs://skynamo_history/temp")

    cmd = ['gcloud', 'storage', 'ls', 'gs://skynamo_history/temp/']
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        files = result.stdout

        files = files.splitlines()
        for i, file in enumerate(files):
            print(f"{i + 1}/{len(files)}")
            schema_id = int(file.split('/')[-1].split('_')[0])
            schema_string = ','.join([f'{column}:{csv_schemas[schema_id][column]}'
                                      for column in sorted(csv_schemas[schema_id])])
            os.system(f"bq load {bq_load_options} --schema={schema_string} {bq_dest} {file}")
    else:
        print(result.stderr)

    os.system("gcloud storage rm -r gs://skynamo_history/temp/")

    shutil.rmtree(output_dir)
    shutil.rmtree(combined_dir)
    if c_storage_source:
        shutil.rmtree(input_source)


def find_temp_dir():
    for i in range(1, 50):
        if os.path.exists(f'temp_{i}'):
            continue
        else:
            os.mkdir(f'temp_{i}')
            output_dir = f'temp_{i}'
            return output_dir
    else:
        raise OSError('All temp directories are in use')


if __name__ == '__main__':
    parser = ArgumentParser(description='Upload a directory of xlsx files to a BigQuery table')
    parser.add_argument('source', type=str,
                        help='The local or cloud directory source, recursive: path/to/directory or gs://bucket/path')
    parser.add_argument('table', type=str, help='The output dataset and table name: "dataset.table"')
    parser.add_argument('--recover', action='store_true', help='Recover from a previous run')
    args = parser.parse_args()

    if args.source.startswith('gs://'):
        convert_and_upload(args.table, c_storage_source=args.source)
    else:
        convert_and_upload(args.table, local_source=args.source, recover=args.recover)
