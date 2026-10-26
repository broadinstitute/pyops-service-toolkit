"""Utility classes for TDR schema."""

import logging
import re
import time
import numpy as np
import pandas as pd
import math
from datetime import date, datetime
from typing import Any, Iterable, Optional, Union

GCP_FILEREF_REGEX = "^gs://.*"
"""@private"""


def _is_null(value: Any) -> bool:
    """Whether a value counts as null (None or a float NaN), the same way pandas does for scalar cells."""
    return value is None or (isinstance(value, float) and math.isnan(value))


def _is_number(value: Any) -> bool:
    """Whether a value is an int or float (booleans are not considered numbers)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_integer_like(value: Union[int, float]) -> bool:
    """Whether a number is an int, or a float with no fractional part. NaN is ignored (treated as integer-like)."""
    if isinstance(value, float):
        return math.isnan(value) or value.is_integer()
    return True


class _ColumnProfile:
    """
    Running summary of everything needed to infer the TDR schema for one column.

    Values are added one at a time so the schema can be inferred in a single pass over the data without ever
    holding a whole column in memory. The tracked state mirrors what `InferTDRSchema` derives from the full
    list of values for a column when it works from a DataFrame.

    Two notions of "empty" are tracked, matching the DataFrame path: `non_null_count` only ignores `None`/NaN
    (used to decide if a column is required), while everything else ignores empty/whitespace-only strings as well
    (used to decide a column's data type).
    """

    __slots__ = (
        "non_null_count", "typed_count", "scalar_count", "list_count", "any_truthy", "has_fileref",
        "first_value_is_list", "first_scalar_type", "scalars_match_first_type", "scalars_all_numeric",
        "scalars_all_integer_like", "first_list_item_type", "list_items_match_first_type",
        "list_items_all_numeric", "list_items_all_integer_like",
    )

    def __init__(self) -> None:
        self.non_null_count = 0
        self.typed_count = 0
        self.scalar_count = 0
        self.list_count = 0
        # Whether any value used for typing is truthy (0, "", False and [] are not)
        self.any_truthy = False
        self.has_fileref = False
        self.first_value_is_list: Optional[bool] = None
        self.first_scalar_type: Optional[type] = None
        self.scalars_match_first_type = True
        self.scalars_all_numeric = True
        self.scalars_all_integer_like = True
        self.first_list_item_type: Optional[type] = None
        self.list_items_match_first_type = True
        self.list_items_all_numeric = True
        self.list_items_all_integer_like = True

    def add(self, value: Any) -> None:
        """Add one value for the column to the profile."""
        if _is_null(value):
            return
        self.non_null_count += 1
        # Empty/whitespace-only strings are ignored when determining data types
        if isinstance(value, str) and not value.strip():
            return
        self.typed_count += 1
        if self.first_value_is_list is None:
            self.first_value_is_list = isinstance(value, list)
        if value:
            self.any_truthy = True
        if isinstance(value, list):
            self.list_count += 1
            for item in value:
                self._add_list_item(item)
        else:
            self.scalar_count += 1
            self._add_scalar(value)

    def _add_scalar(self, value: Any) -> None:
        if self.first_scalar_type is None:
            self.first_scalar_type = type(value)
        elif not isinstance(value, self.first_scalar_type):
            self.scalars_match_first_type = False
        if _is_number(value):
            if not _is_integer_like(value):
                self.scalars_all_integer_like = False
        else:
            self.scalars_all_numeric = False
            if isinstance(value, str) and re.search(pattern=GCP_FILEREF_REGEX, string=value):
                self.has_fileref = True

    def _add_list_item(self, item: Any) -> None:
        if self.first_list_item_type is None:
            self.first_list_item_type = type(item)
        elif not isinstance(item, self.first_list_item_type):
            self.list_items_match_first_type = False
        if _is_number(item):
            if not _is_integer_like(item):
                self.list_items_all_integer_like = False
        else:
            self.list_items_all_numeric = False
            if isinstance(item, str) and re.search(pattern=GCP_FILEREF_REGEX, string=item):
                self.has_fileref = True

    @property
    def is_type_consistent(self) -> bool:
        """Whether all the values used for typing are of the same type."""
        if self.list_count and self.scalar_count:
            # Some values are lists and some are not
            return False
        if self.list_count:
            return self.list_items_match_first_type
        # A column made up of only numbers (ints and floats) is consistent, which matches pandas upcasting a column
        # of ints and floats to all floats. This does not happen if the column also holds empty strings
        numbers_upcast_to_float = self.scalars_all_numeric and self.non_null_count == self.typed_count
        return self.scalars_match_first_type or numbers_upcast_to_float


class InferTDRSchema:
    """A class to infer the schema for a table in TDR (Terra Data Repository) based on input metadata."""

    PYTHON_TDR_DATA_TYPE_MAPPING = {
        str: "string",
        "fileref": "fileref",
        bool: "boolean",
        bytes: "bytes",
        date: "date",
        datetime: "datetime",
        float: "float64",
        np.float64: "float64",
        int: "int64",
        np.int64: "int64",
        time: "time",
    }
    """@private"""

    def __init__(
            self,
            input_metadata: Iterable[dict],
            table_name: str,
            all_fields_non_required: bool = False,
            allow_disparate_data_types_in_column: bool = False,
            primary_key: Optional[str] = None
    ):
        """
        Initialize the InferTDRSchema class.

        **Args:**
        - input_metadata (Iterable[dict]): The input metadata to infer the schema from. If this is a `list` the whole
                list is loaded into a DataFrame to infer the schema. Any other iterable (e.g. a generator) is read in
                a single pass without holding the rows in memory, which makes it possible to infer the schema for
                very large inputs. Note that an iterator can only be consumed once.
        - table_name (str): The name of the table for which the schema is being inferred.
        - all_fields_non_required (bool): A boolean indicating whether all columns should be set to non-required
                besides for primary key. Defaults to `False`
        - allow_disparate_data_types_in_column (bool): A boolean indicating whether force disparate data types in a
                column to be of type `str` Defaults to `False`.
        - primary_key (str, optional): The name of the primary key column. Used to determine if the column
            should be required
        """
        self.input_metadata = input_metadata
        """@private"""
        self.table_name = table_name
        """@private"""
        self.all_fields_non_required = all_fields_non_required
        """@private"""
        self.primary_key = primary_key
        """@private"""
        self.allow_disparate_data_types_in_column = allow_disparate_data_types_in_column
        """@private"""

    @staticmethod
    def _treat_empty_as_none(values: list[Any]) -> list[Any]:
        """Replace empty/whitespace-only strings with None so they are ignored when determining data types."""
        return [None if isinstance(v, str) and not v.strip() else v for v in values]

    def _check_type_consistency(self, key_value_type_mappings: dict) -> list[dict]:
        """
        Check if all values for each header are of the same type.

        **Args:**
        - key_value_type_mappings (dict): A dictionary where the key is the header,
                and the value is a list of values for the header.

        Raises:
            Exception: If types do not match for any header.
        """
        matching = []

        disparate_header_info = []

        for header, values_for_header in key_value_type_mappings.items():
            # ignore empty strings when checking types, same as "None" entries
            values_for_header = self._treat_empty_as_none(values_for_header)
            # check if some values are lists while others are not (consider this a "mismatch" if so) while ignoring
            # "None" entries
            if (any(isinstance(item, list) for item in values_for_header if item is not None) and
                    not all(isinstance(item, list) for item in values_for_header if item is not None)):
                all_values_matching = False
            # if the row contains ONLY lists of items, check that all items in each list are of the same type (while
            # ignoring "None" entries)
            elif all(isinstance(item, list) for item in values_for_header if item is not None):
                # first get all substrings that have some values
                non_empty_substrings = [v for v in values_for_header if v]
                if non_empty_substrings:
                    # get one "type" from the list of values
                    first_match_type = type([v[0] for v in non_empty_substrings][0])
                    all_values_matching = all(
                        all(isinstance(item, first_match_type) for item in sublist) for sublist in non_empty_substrings
                    )
                else:
                    # if all "sub-lists" are empty, assume that all types are matching (all empty lists are handled
                    # below)
                    all_values_matching = True
            else:
                # find one value that's non-none to get the type to check against
                # specifically check if not "None" since we can have all zeroes, for example
                type_to_match_against = type([v for v in values_for_header if v is not None][0])
                # check if all the values in the list that are non-none match the type of the first entry
                all_values_matching = all(
                    isinstance(v, type_to_match_against) for v in values_for_header if v is not None
                )

            # If ALL rows for the header are none, force the type to be a string
            if all_values_matching and not any(values_for_header):
                matching.append({header: all_values_matching})
                disparate_header_info.append(
                    {
                        "header": header,
                        "force_to_string": True,
                    }
                )
            if not all_values_matching and self.allow_disparate_data_types_in_column:
                logging.info(
                    f"Not all data types matched for header '{header}' but forcing them to strings as "
                    f"'allow_disparate_data_types_in_column' setting is set to true"
                )
                matching.append({header: True})
                disparate_header_info.append(
                    {
                        "header": header,
                        "force_to_string": True,
                    }
                )
            else:
                matching.append({header: all_values_matching})  # type: ignore[dict-item]
                disparate_header_info.append(
                    {
                        "header": header,
                        "force_to_string": False,
                    }
                )

        # Returns true if all headers are determined to be "matching"
        problematic_headers = [
            d.keys()
            for d in matching
            if not list(d.values())[0]
        ]

        if problematic_headers:
            raise Exception(
                f"Not all values for the following headers are of the same type: {problematic_headers}. To force all"
                f" values in rows of a given column to be forced to the same type and bypass this error, re-run with "
                f"the 'force_disparate_rows_to_string' option set to true"
            )

        return disparate_header_info

    @staticmethod
    def _interpret_number(x: Union[float, int]) -> Union[int, float]:
        if isinstance(x, float) and x.is_integer():
            return int(x)
        return x

    def _determine_if_float_or_int(self, interpreted_numbers: list[Union[int, float]]) -> str:
        # Remove NaNs before type checks
        non_nan_numbers = [x for x in interpreted_numbers if not (isinstance(x, float) and math.isnan(x))]

        # If all values are int, return int type
        if all(isinstance(row_value, int) for row_value in non_nan_numbers):
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[int]
        # If ANY or ALL values are float, return float type
        return self.PYTHON_TDR_DATA_TYPE_MAPPING[float]

    def _python_type_to_tdr_type_conversion(self, values_for_header: list[Any]) -> str:
        """
        Convert Python data types to TDR data types.

        Args:
            values_for_header (Any): All values for a column header.

        Returns:
            str: The TDR data type.
        """
        gcp_fileref_regex = "^gs://.*"

        # Collect all the non-None values for the column
        non_none_values = [v for v in values_for_header if v is not None]

        # HANDLE SPECIAL CASES

        # FILE REFS AND LISTS OF FILE REFS
        # If ANY of the values for a header are of type "fileref", we assume that the column is a fileref
        for row_value in non_none_values:
            if isinstance(row_value, str) and re.search(pattern=gcp_fileref_regex, string=row_value):
                return self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]

            if isinstance(row_value, list):
                # Check for a potential array of filerefs - if ANY of the items in a list are
                # of type "fileref" we assume that the whole column is a fileref
                for item in row_value:
                    if isinstance(item, str) and re.search(pattern=gcp_fileref_regex, string=item):
                        return self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]

        # INTEGERS/FLOATS AND LISTS OF INTEGERS AND FLOATS
        # Case 1: All values are plain numbers (int or float) - specifically excluding bools
        if all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in non_none_values):
            interpreted_numbers = [self._interpret_number(row_value) for row_value in non_none_values]
            return self._determine_if_float_or_int(interpreted_numbers)

        # Case 2: Values are lists of numbers (e.g., [[1, 2], [3.1], [4]])
        if all(isinstance(row_value, list) for row_value in non_none_values):
            if all(
                    all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in row_value)
                    for row_value in non_none_values
            ):
                # Flatten the list of lists and interpret all non-None elements
                interpreted_numbers = [self._interpret_number(item)
                                       for row_value in non_none_values for item in row_value if item is not None]

                return self._determine_if_float_or_int(interpreted_numbers)

        # If none of the above special cases apply, use the first of the non-null values to determine the
        # TDR data type
        first_value = non_none_values[0]
        if isinstance(first_value, list):
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(first_value[0])]
        return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(first_value)]

    def _format_column_metadata(self, key_value_type_mappings: dict, disparate_header_info: list[dict]) -> list[dict]:
        """
        Generate the metadata for each column's header name, data type, and whether it's an array of values.

        Args:
            key_value_type_mappings (dict): A dictionary where the key is the header,
                and the value is a list of values for the header.

        Returns:
            list[dict]: A list of dictionaries containing column metadata.
        """
        columns = []

        for header, values_for_header in key_value_type_mappings.items():
            # ignore empty strings when determining data types, same as "None" entries
            values_for_header = self._treat_empty_as_none(values_for_header)
            force_to_string = [h["force_to_string"] for h in disparate_header_info if h["header"] == header][0]

            # if the ANY of the values for a given header is a list, we assume that column contains arrays of values
            array_of = True if any(isinstance(v, list) for v in values_for_header) else False

            string_type = self.PYTHON_TDR_DATA_TYPE_MAPPING[str]
            fileref_type = self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]

            if not any(v is not None for v in values_for_header):
                # nothing to infer a type from (every value is empty), so default to string
                data_type = string_type
            else:
                data_type = self._python_type_to_tdr_type_conversion(values_for_header)
                # a column containing any file references must stay a fileref, even if other values are mismatched
                if force_to_string and data_type != fileref_type:
                    logging.info(f"Header '{header}' was forced to string to to mismatched datatypes in column")
                    data_type = string_type

            column_metadata = {
                "name": header,
                "datatype": data_type,
                "array_of": array_of,
            }
            columns.append(column_metadata)

        return columns

    def _gather_required_and_non_required_headers(self, metadata_df: Any, dataframe_headers: list[str]) -> list[dict]:
        """
        Determine whether each header is required or not.

        Args:
            metadata_df (Any): The original dataframe.
            dataframe_headers (list[str]): A list of dataframe headers.

        Returns:
            list[dict]: A list of dictionaries containing header requirements.
        """
        header_requirements = []

        na_replaced = metadata_df.replace({None: np.nan})
        for header in dataframe_headers:
            all_none = na_replaced[header].isna().all()
            some_none = na_replaced[header].isna().any()
            contains_array = na_replaced[header].apply(lambda x: isinstance(x, (np.ndarray, list))).any()

            # if the column contains any arrays, set it as optional since arrays cannot be required in tdr
            if contains_array:
                header_requirements.append({"name": header, "required": False})
            # if all rows are none for a given column, we set the default type to "string" type in TDR
            elif all_none:
                header_requirements.append({"name": header, "required": False, "data_type": "string"})
            # if some rows are none or all non required is set to true AND header
            # is not primary key, we set the column to non-required
            elif some_none or (self.all_fields_non_required and header != self.primary_key):
                header_requirements.append({"name": header, "required": False})
            else:
                header_requirements.append({"name": header, "required": True})

        return header_requirements

    @staticmethod
    def _reformat_metadata(cleaned_metadata: list[dict]) -> dict:
        """
        Create a dictionary where the key is the header name, and the value is a list of all values for that header.

        Args:
            cleaned_metadata (list[dict]): The cleaned metadata.

        Returns:
            dict: A dictionary with header names as keys and lists of values as values.
        """
        key_value_type_mappings = {}
        unique_headers = {key for row in cleaned_metadata for key in row}

        for header in unique_headers:
            for row in cleaned_metadata:
                value = row[header]
                if header not in key_value_type_mappings:
                    key_value_type_mappings[header] = [value]
                else:
                    key_value_type_mappings[header].append(value)
        return key_value_type_mappings

    def infer_schema(self) -> dict:
        """
        Infer the schema for the table based on the input metadata.

        If the input metadata is a `list` it is loaded into a DataFrame. Any other iterable is processed in a single
        streaming pass so that memory use does not grow with the number of rows.

        **Returns:**
        - dict: The inferred schema in the format expected by TDR.
        """
        logging.info(f"Inferring schema for table {self.table_name}")
        if isinstance(self.input_metadata, list):
            return self._infer_schema_from_dataframe()
        return self._infer_schema_streaming()

    @staticmethod
    def _rename_header(header: str) -> str:
        """Strip the "entity:" prefix from headers such as "entity:sample_id". Other headers are left alone."""
        return header.split(":", 1)[1] if header.startswith("entity:") else header

    def _resolve_streamed_data_type(self, profile: _ColumnProfile) -> str:
        """Equivalent of `_python_type_to_tdr_type_conversion` for a column that was profiled while streaming."""
        if profile.has_fileref:
            return self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]
        # All values are plain numbers
        if profile.scalar_count == profile.typed_count and profile.scalars_all_numeric:
            all_integers = profile.scalars_all_integer_like
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[int if all_integers else float]
        # All values are lists of plain numbers
        if profile.list_count == profile.typed_count and profile.list_items_all_numeric:
            all_integers = profile.list_items_all_integer_like
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[int if all_integers else float]
        # Otherwise use the type of the first value (or of the first item if the first value is a list)
        first_type = profile.first_list_item_type if profile.first_value_is_list else profile.first_scalar_type
        if first_type is None:
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[str]
        return self.PYTHON_TDR_DATA_TYPE_MAPPING[first_type]

    def _infer_schema_streaming(self) -> dict:
        """
        Infer the schema from a single pass over the input metadata, keeping only a small summary of each column.

        Produces the same schema as `_infer_schema_from_dataframe`.
        """
        profiles: dict[str, _ColumnProfile] = {}
        total_rows = 0
        for row in self.input_metadata:
            total_rows += 1
            for header, value in row.items():
                profile = profiles.get(header)
                if profile is None:
                    profile = profiles[header] = _ColumnProfile()
                profile.add(value)

        column_metadata = []
        problematic_headers = []
        for original_header, profile in profiles.items():
            header = self._rename_header(original_header)

            # Determine whether the column is required
            # arrays cannot be required in tdr
            if profile.list_count:
                header_metadata: dict[str, Any] = {"name": header, "required": False}
            # all rows are none for the column so default to string
            elif profile.non_null_count == 0:
                header_metadata = {"name": header, "required": False, "data_type": "string"}
            elif profile.non_null_count < total_rows or (self.all_fields_non_required and header != self.primary_key):
                header_metadata = {"name": header, "required": False}
            else:
                header_metadata = {"name": header, "required": True}

            # Columns where every value is none have nothing to infer a type from and are not included in the schema
            if profile.non_null_count == 0:
                column_metadata.append(header_metadata)
                continue

            # Determine whether the types in the column are consistent and if they need to be forced to a string
            force_to_string = False
            if profile.is_type_consistent:
                # a column where every value is falsy (e.g. all zeros) is forced to a string. A column of only numbers
                # with some nulls is loaded into a DataFrame as floats, where the nulls become NaN, which is truthy
                has_nan_from_nulls = (
                    profile.non_null_count < total_rows
                    and profile.non_null_count == profile.scalar_count == profile.typed_count
                    and profile.scalars_all_numeric
                )
                force_to_string = not (profile.any_truthy or has_nan_from_nulls)
            elif self.allow_disparate_data_types_in_column:
                logging.info(
                    f"Not all data types matched for header '{header}' but forcing them to strings as "
                    f"'allow_disparate_data_types_in_column' setting is set to true"
                )
                force_to_string = True
            else:
                problematic_headers.append(header)

            if profile.typed_count == 0:
                # nothing to infer a type from (every value is empty), so default to string
                data_type = self.PYTHON_TDR_DATA_TYPE_MAPPING[str]
            else:
                data_type = self._resolve_streamed_data_type(profile)
                # a column containing any file references must stay a fileref, even if other values are mismatched
                if force_to_string and data_type != self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]:
                    logging.info(f"Header '{header}' was forced to string to to mismatched datatypes in column")
                    data_type = self.PYTHON_TDR_DATA_TYPE_MAPPING[str]

            header_metadata.update({"name": header, "datatype": data_type, "array_of": profile.list_count > 0})
            column_metadata.append(header_metadata)

        if problematic_headers:
            raise Exception(
                f"Not all values for the following headers are of the same type: {problematic_headers}. To force all"
                f" values in rows of a given column to be forced to the same type and bypass this error, re-run with "
                f"the 'force_disparate_rows_to_string' option set to true"
            )

        return {"name": self.table_name, "columns": column_metadata}

    def _infer_schema_from_dataframe(self) -> dict:
        """Infer the schema by loading all of the input metadata into a DataFrame."""
        # create the dataframe
        metadata_df = pd.DataFrame(self.input_metadata)
        # Replace all nan with None
        metadata_df = metadata_df.where(pd.notnull(metadata_df), None)

        # find all headers that need to be renamed if they start with "entity:" (e.g. "entity:sample_id") and rename
        # the headers. Headers that merely start with "entity" (e.g. "entity_type") are left alone.
        headers_to_be_renamed = {
            h: h.split(":", 1)[1] for h in list(metadata_df.columns) if h.startswith("entity:")
        }
        metadata_df = metadata_df.rename(columns=headers_to_be_renamed)

        # start by gathering the column metadata by determining which headers are required or not
        column_metadata = self._gather_required_and_non_required_headers(metadata_df, list(metadata_df.columns))

        # drop columns where ALL values are None, but keep rows where some values are None
        # we keep the rows where some values are none because if we happen to have a different column that's none in
        # every row, we could end up with no data at the end
        all_none_columns_dropped_df = metadata_df.dropna(axis=1, how="all")
        cleaned_metadata = all_none_columns_dropped_df.to_dict(orient="records")
        key_value_type_mappings = self._reformat_metadata(cleaned_metadata)

        # check to see if all values corresponding to a header are of the same type
        disparate_header_info = self._check_type_consistency(key_value_type_mappings)

        columns = self._format_column_metadata(
            key_value_type_mappings=key_value_type_mappings, disparate_header_info=disparate_header_info
        )

        # combine the information about required headers with the data types that were collected
        for header_metadata in column_metadata:
            matching_metadata = [d for d in columns if d["name"] == header_metadata["name"]]
            if matching_metadata:
                header_metadata.update(matching_metadata[0])

        tdr_tables_json = {
            "name": self.table_name,
            "columns": column_metadata,
        }

        return tdr_tables_json
