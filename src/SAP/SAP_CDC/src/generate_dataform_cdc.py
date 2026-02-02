
import json
import logging
import os
import pathlib
import sys
import yaml
from pathlib import Path
from string import Template
from concurrent import futures

from google.cloud import bigquery
from jinja2 import Template as JinjaTemplate

# Add parent directories to sys.path to allow importing common modules
sys.path.append(str(pathlib.Path(__file__).parent.parent)) 
sys.path.append(str(pathlib.Path(__file__).parent.parent.parent.parent)) # src/

from common.py_libs import cortex_bq_client
from common.py_libs.configs import load_config_file_from_env

# Constants
_CONFIG_FILE = "src/SAP/SAP_CDC/cdc_settings.yaml"
_DATAFORM_CDC_DIR = "dataform/definitions/cdc"
_DATAFORM_SOURCES_DIR = "dataform/definitions/sources"
_CDC_TEMPLATE_FILE = "src/SAP/SAP_CDC/src/template_dataform/cdc_operations.sqlx"

def generate_source_declaration(raw_dataset_name, table_names, output_dir):
    """Generates a JS file declaring all source tables."""
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{output_dir}/raw_sap.js"
    
    js_content = f"""// Declaration of RAW tables
// This allows referencing them via ref('table_name')

const raw_dataset = "{raw_dataset_name}";

"""
    for table_name in table_names:
        js_content += f"""declare({{
  database: dataform.projectConfig.defaultDatabase,
  schema: raw_dataset,
  name: "{table_name}"
}});

"""
    with open(filename, "w") as f:
        f.write(js_content)
    logging.info(f"Generated source declarations in {filename}")

# Columns to be excluded from the update/insert list (but not necessarily from selection)
_CDC_EXCLUDED_COLUMN_LIST = ['_PARTITIONTIME', 'operation_flag', 'is_deleted', 'recordstamp']

client = cortex_bq_client.CortexBQClient()

def get_primary_keys(full_table_name):
    """Retrieves primary key columns for raw table from metadata table DD03L."""
    try:
        _, dataset, table_name = full_table_name.split('.')
        query = (f'SELECT fieldname '
                 f'FROM `{dataset}.dd03l` '
                 f'WHERE KEYFLAG = "X" AND fieldname != ".INCLUDE" '
                 f'AND tabname = "{table_name.upper()}"')
        query_job = client.query(query)
        fields = []
        for row in query_job:
            fields.append(row['fieldname'])
        return fields
    except Exception as e:
        if os.environ.get("MOCK_BQ"):
            logging.warning(f"Mocking primary keys for {full_table_name}")
            return ['MANDT', 'KEY1']
        logging.error(f"Error getting primary keys for {full_table_name}: {e}")
        return []

def generate_cdc_sqlx(raw_table_name, cdc_table_name, target_dataset, base_table_name):
    """Generates the SQLX content for a CDC table."""
    
    primary_keys = get_primary_keys(raw_table_name)
    if not primary_keys:
        logging.warning(f"Skipping {raw_table_name}: No primary keys found.")
        return None

    # Get schema to build columns list
    if os.environ.get("MOCK_BQ"):
        # Dummy schema for testing
        from collections import namedtuple
        Field = namedtuple('Field', ['name'])
        schema = [Field('MANDT'), Field('KEY1'), Field('COL1'), Field('COL2'), Field('recordstamp'), Field('operation_flag')]
    else:
        try:
            table_ref = client.get_table(raw_table_name)
            schema = table_ref.schema
        except Exception as e:
            logging.error(f"Error getting schema for {raw_table_name}: {e}")
            return None

    fields = [f'`{f.name}`' for f in schema if f.name not in _CDC_EXCLUDED_COLUMN_LIST]
    
    # Logic for MVCC (Multi-Version Concurrency Control) / Merging
    # We replicate the logic from cdc_sql_template.sql
    
    primary_keys_join = " AND ".join([f'T.`{pk}` = S.`{pk}`' for pk in primary_keys])
    primary_keys_partition = ", ".join([f'`{pk}`' for pk in primary_keys])
    
    # Construct UPDATE clause
    # Construct UPDATE clause
    update_set_clause = ", ".join([f'T.`{f.name}` = S.`{f.name}`' for f in schema if f.name not in _CDC_EXCLUDED_COLUMN_LIST])
    insert_cols = ", ".join([f'`{f.name}`' for f in schema if f.name not in _CDC_EXCLUDED_COLUMN_LIST])
    insert_vals = ", ".join([f'S.`{f.name}`' for f in schema if f.name not in _CDC_EXCLUDED_COLUMN_LIST])

    partition_by_keys = [f'`{pk}`' for pk in primary_keys]

    with open(_CDC_TEMPLATE_FILE, "r") as f:
        template_str = f.read()

    template = JinjaTemplate(template_str)
    
    sqlx_content = template.render(
        target_schema=target_dataset.split('.')[-1],
        base_table=base_table_name,
        partition_by_keys=partition_by_keys,
        join_condition=primary_keys_join,
        insert_cols=insert_cols,
        insert_vals=insert_vals,
        update_set_clause=update_set_clause
    )
    return sqlx_content

def main():
    logging.basicConfig(level=logging.INFO)
    
    # Load Main Config
    config_path = os.environ.get("CONFIG_FILE", "config/config.json")
    try:
        # Load directly using the file path, avoiding the Cloud Build specific env file loader
        from common.py_libs.configs import load_config_file
        cortex_config = load_config_file(config_path)
    except Exception as e:
        logging.error(f"Failed to load cortex config from {config_path}: {e}")
        return

    orch_mode = cortex_config.get("orchestrationMode", "dags").lower()
    if orch_mode != "dataform":
        logging.info(f"Skipping Dataform generation as orchestrationMode is '{orch_mode}'")
        return

    source_project = cortex_config.get("projectIdSource")
    target_project = cortex_config.get("projectIdTarget") # CDC usually stays in source project or specific CDC project
    
    if not source_project:
        logging.error("projectIdSource is missing in config.json")
        return

    # Extract dataset names from config struct
    # Assuming standard structure SAP.datasets.raw / SAP.datasets.cdc
    try:
        raw_dataset_name = cortex_config["SAP"]["datasets"]["raw"]
        cdc_dataset_name = cortex_config["SAP"]["datasets"]["cdc"]
    except KeyError:
        logging.error("SAP dataset configuration missing in config.json")
        return

    full_raw_dataset = f"{source_project}.{raw_dataset_name}"
    full_cdc_dataset = f"{source_project}.{cdc_dataset_name}"

    logging.info(f"Generating CDC for {full_raw_dataset} -> {full_cdc_dataset}")

    # Load CDC Settings
    sql_flavour = cortex_config["SAP"].get("SQLFlavor", "ECC")
    
    with open(_CONFIG_FILE, encoding="utf-8") as f:
        # Render template if needed (for Jinja inside yaml)
        content = f.read()
        t = JinjaTemplate(content)
        rendered = t.render(sql_flavour=sql_flavour)
        settings = yaml.safe_load(rendered)

    table_configs = settings.get("data_to_replicate", [])
    
    os.makedirs(_DATAFORM_CDC_DIR, exist_ok=True)

    os.makedirs(_DATAFORM_CDC_DIR, exist_ok=True)
    
    processed_raw_tables = []

    for config in table_configs:
        base_table = config["base_table"]
        target_table = config.get("target_table", base_table)
        
        full_raw_table = f"{full_raw_dataset}.{base_table}"
        full_cdc_table = f"{full_cdc_dataset}.{target_table}"
        
        logging.info(f"Processing {base_table}...")
        
        # Pass base_table name for ref() usage
        sqlx = generate_cdc_sqlx(full_raw_table, full_cdc_table, full_cdc_dataset, base_table)
        
        if sqlx:
            processed_raw_tables.append(base_table)
            filename = f"{_DATAFORM_CDC_DIR}/{target_table}.sqlx"
            with open(filename, "w") as f:
                f.write(sqlx)
            logging.info(f"Generated {filename}")

    # Generate source declarations for all successfully processed tables
    if processed_raw_tables:
        generate_source_declaration(raw_dataset_name, processed_raw_tables, _DATAFORM_SOURCES_DIR)

if __name__ == "__main__":
    main()
