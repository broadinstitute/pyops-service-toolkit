"""Module for interacting with TDR tables."""
import json
import logging
import sys

from ..tdr_utils.tdr_api_utils import TDR
from ..tdr_utils.tdr_schema_utils import InferTDRSchema


class SetUpTDRTables:
    """A class to set up TDR tables by comparing and updating schemas."""

    def __init__(
            self,
            tdr: TDR,
            dataset_id: str,
            table_info_dict: dict,
            all_fields_non_required: bool = False,
            force_disparate_rows_to_string: bool = False,
            ignore_existing_schema_mismatch: bool = False
    ):
        """
        Initialize the SetUpTDRTables class.

        **Args:**
        - tdr (`ops_utils.tdr_utils.tdr_api_utils.TDR`): An instance of the TDR class.
        - dataset_id (str): The ID of the dataset.
        - table_info_dict (dict): A dictionary containing table information.
        - all_fields_non_required (bool): A boolean indicating whether all columns are non-required.
        Defaults to `False`.
        - force_disparate_rows_to_string (bool): A boolean indicating whether disparate rows should be forced to
                string. Defaults to `False`.
        - ignore_existing_schema_mismatch (bool): A boolean indicating whether to not fail on data type not
                matching existing schema. Defaults to `False`.
        """
        self.tdr = tdr
        """@private"""
        self.dataset_id = dataset_id
        """@private"""
        self.table_info_dict = table_info_dict
        """@private"""
        self.all_fields_non_required = all_fields_non_required
        """@private"""
        self.force_disparate_rows_to_string = force_disparate_rows_to_string
        """@private"""
        self.ignore_existing_schema_mismatch = ignore_existing_schema_mismatch
        """@private"""

    @staticmethod
    def _compare_table(reference_dataset_table: dict, target_dataset_table: list[dict], table_name: str) -> list[dict]:
        """
        Compare tables between two datasets.

        Args:
            reference_dataset_table (dict): The reference dataset table schema.
            target_dataset_table (list[dict]): The target dataset table schema.
            table_name (str): The name of the table being compared.

        Returns:
            list[dict]: A list of columns that need to be updated.
        """
        logging.info(f"Comparing table {reference_dataset_table['name']} to existing target table")
        columns_to_update = []
        # Convert target table to dict for easier comparison
        target_dataset_table_dict = {col["name"]: col for col in target_dataset_table}
        # Go through each column in reference table and see if it exists and if so, is it the same in target table
        for column_dict in reference_dataset_table["columns"]:
            # Check if column exists in target table already
            if column_dict["name"] not in target_dataset_table_dict.keys():
                column_dict["action"] = "add"
                columns_to_update.append(column_dict)
            else:
                # Check if column exists but is not set up the same
                if column_dict != target_dataset_table_dict[column_dict["name"]]:
                    column_dict["action"] = "modify"
                    logging.warning(
                        f'Column {column_dict["name"]} in table {table_name} does not match. Expected column info:\n'
                        f'{json.dumps(column_dict, indent=4)}\nexisting column info:\n'
                        f'{json.dumps(target_dataset_table_dict[column_dict["name"]], indent=4)}'
                    )
                    columns_to_update.append(column_dict)
        return columns_to_update

    def run(self) -> dict:
        """
        Run the setup process to ensure tables are created or updated as needed.

        **Returns:**
        - dict: A dictionary with table names as keys and column information as values.
        """
        dataset_info = self.tdr.get_dataset_info(dataset_id=self.dataset_id, info_to_include=["SCHEMA"]).json()
        existing_tdr_table_schema_info = {
            table_dict["name"]: table_dict["columns"]
            for table_dict in dataset_info["schema"]["tables"]
        }
        tables_to_create = []
        valid = True
        # Loop through all expected tables to see if exist and match schema. If not then create one.
        for ingest_table_name, ingest_table_dict in self.table_info_dict.items():
            primary_key = ingest_table_dict.get("primary_key")

            # Get TDR schema info for tables to ingest
            expected_tdr_schema_dict = InferTDRSchema(
                input_metadata=ingest_table_dict["ingest_metadata"],
                table_name=ingest_table_name,
                all_fields_non_required=self.all_fields_non_required,
                primary_key=primary_key,
                allow_disparate_data_types_in_column=self.force_disparate_rows_to_string,
            ).infer_schema()

            # If unique id then add to table json
            if primary_key:
                expected_tdr_schema_dict["primaryKey"] = [ingest_table_dict["primary_key"]]

            # add table to ones to create if it does not exist
            if ingest_table_name not in existing_tdr_table_schema_info:
                # Ensure there is columns in table before adding to list
                if expected_tdr_schema_dict['columns']:
                    tables_to_create.append(expected_tdr_schema_dict)
            else:
                # Compare columns
                columns_to_update = self._compare_table(
                    reference_dataset_table=expected_tdr_schema_dict,
                    target_dataset_table=existing_tdr_table_schema_info[ingest_table_name],
                    table_name=ingest_table_name
                )
                if columns_to_update:
                    # If any updates needed nothing is done for whole ingest
                    valid = False
                    for column_to_update_dict in columns_to_update:
                        logging.warning(f"Column {column_to_update_dict['name']} needs updates in {ingest_table_name}")
                else:
                    logging.info(f"Table {ingest_table_name} exists and is up to date")
        if valid:
            #  Does nothing with relationships for now
            if tables_to_create:
                tables_string = ", ".join(
                    [table["name"] for table in tables_to_create]
                )
                logging.info(f"Table(s) {tables_string} do not exist in dataset. Will attempt to create")
                self.tdr.update_dataset_schema(
                    dataset_id=self.dataset_id,
                    update_note=f"Creating tables in dataset {self.dataset_id}",
                    tables_to_add=tables_to_create
                )
            else:
                logging.info("All tables in dataset exist and are up to date")
        else:
            logging.warning("Tables do not appear to be valid")
            if self.ignore_existing_schema_mismatch:
                logging.warning("Ignoring schema mismatch because ignore_existing_schema_mismatch was used")
            else:
                logging.error(
                    "Tables need manual updating. If want to force through use ignore_existing_schema_mismatch."
                )
                sys.exit(1)
        # Return schema info for all existing tables after creation
        dataset_info = self.tdr.get_dataset_info(dataset_id=self.dataset_id, info_to_include=["SCHEMA"]).json()
        # Return dict with key being table name and value being dict of columns with key being
        # column name and value being column info
        return {
            table_dict["name"]: {
                column_dict["name"]: column_dict
                for column_dict in table_dict["columns"]
            }
            for table_dict in dataset_info["schema"]["tables"]
        }


class MatchSchemas:
    """A class to match schemas between two TDR datasets and add missing tables."""

    def __init__(self, orig_dataset_info: dict, dest_dataset_info: dict, dest_dataset_id: str, tdr: TDR):
        """
        Initialize the MatchSchemas class.

        Args:
            orig_dataset_info (dict): The original dataset information.
            dest_dataset_info (dict): The destination dataset information.
            dest_dataset_id (str): The ID of the destination dataset.
            tdr (TDR): An instance of the TDR class.
        """
        self.orig_dataset_info = orig_dataset_info
        self.dest_dataset_info = dest_dataset_info
        self.dest_dataset_id = dest_dataset_id
        self.tdr = tdr

    def run(self) -> None:
        """
        Run the process to match tables between the original and destination datasets and add missing tables.

        Returns:
            None
        """
        tables_to_update = []
        dest_tables = [table['name'] for table in self.dest_dataset_info["schema"]["tables"]]
        # If table exists already assumes it is the same schema
        for table in self.orig_dataset_info["schema"]["tables"]:
            if table['name'] not in dest_tables:
                logging.info(
                    f"Table {table['name']} not found in new dataset {self.dest_dataset_info['name']}. will add table"
                )
                tables_to_update.append(table)
        if tables_to_update:
            logging.info(
                f"Adding {len(tables_to_update)} tables to new dataset {self.dest_dataset_info['name']}"
            )
            self.tdr.update_dataset_schema(
                dataset_id=self.dest_dataset_id,
                tables_to_add=tables_to_update,
                update_note=f"Adding tables to dataset {self.dest_dataset_info['name']}"
            )