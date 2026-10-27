import contextlib
import random

import pandas as pd
import pytest
from unittest import TestCase

from ops_utils.tdr_utils.tdr_schema_utils import InferTDRSchema


class TestInferTDRSchema(TestCase):

    def setUp(self):
        self.table_name = "fake-table"
        self.test_metadata_uniform = [
            {
                "column_a": "foo",
                "column_b": 100,
                "column_c": ["a", "b", "c"],
            },
            {
                "column_a": "bar",
                "column_b": 3030,
                "column_c": ["d", "e", "f"],
            }
        ]

        self.test_metadata_non_uniform = [
            {
                "column_a": "foo",
                "column_b": "100",
                "column_c": ["a", "b", "c"],
            },
            {
                "column_a": "bar",
                "column_b": 3030,
                "column_c": [1, 2, 3],
            }
        ]

        self.infer_tdr_schema = InferTDRSchema(
            input_metadata=self.test_metadata_uniform,
            table_name=self.table_name,
            all_fields_non_required=False,
            allow_disparate_data_types_in_column=False,
        )

        self.infer_tdr_schema_nonuniform = InferTDRSchema(
            input_metadata=self.test_metadata_non_uniform,
            table_name=self.table_name,
            all_fields_non_required=False,
            allow_disparate_data_types_in_column=False
        )

    def test_infer_schema_uniform_data(self):
        expected_tdr_json = {
            'name': 'fake-table',
            'columns': [
                {'name': 'column_a', 'required': True, 'datatype': 'string', 'array_of': False},
                {'name': 'column_b', 'required': True, 'datatype': 'int64', 'array_of': False},
                {'name': 'column_c', 'required': False, 'datatype': 'string', 'array_of': True}
            ]
        }

        # Run the method
        actual_tdr_json = self.infer_tdr_schema.infer_schema()

        # Assertions
        self.assertEqual(actual_tdr_json, expected_tdr_json)

    def test_infer_schema_non_uniform_data_disparate_not_allowed(self):


        with pytest.raises(Exception) as exc_info:
            # Run the method
            self.infer_tdr_schema_nonuniform.infer_schema()
            assert str(exc_info.value).startswith("Not all values for the following headers are of the same type:")

    def test_infer_schema_non_uniform_data_disparate_allowed(self):
        # Run the method
        actual_tdr_json = InferTDRSchema(
            input_metadata=self.test_metadata_non_uniform,
            table_name=self.table_name,
            all_fields_non_required=False,
            allow_disparate_data_types_in_column=True  # allow disparate data types in column here
        ).infer_schema()

        # Assertions
        expected_tdr_json = {
            'name': 'fake-table',
            'columns': [
                {'name': 'column_a', 'required': True,'datatype': 'string', 'array_of': False},
                {'name': 'column_b', 'required': True, 'datatype': 'string', 'array_of': False},
                {'name': 'column_c', 'required': False, 'datatype': 'string', 'array_of': True}
            ]
        }
        self.assertEqual(actual_tdr_json, expected_tdr_json)

    def test_check_type_consistency(self):
        key_value_mapping = {
            'column_a': ['foo', 'bar'],
            'column_b': [100, 3030],
            'column_c': [['a', 'b', 'c'], ['d', 'e', 'f']]
        }
        actual_disparate_header_info = self.infer_tdr_schema._check_type_consistency(key_value_mapping)
        expected_disparate_header_info = [
            {'header': 'column_a', 'force_to_string': False},
            {'header': 'column_b', 'force_to_string': False},
            {'header': 'column_c', 'force_to_string': False}
        ]
        self.assertEqual(actual_disparate_header_info, expected_disparate_header_info)

    def test_check_type_consistency_disparate(self):
        key_value_mapping = {
            'column_a': ['foo', 'bar'],
            'column_b': ["100", 3030],
            'column_c': [['a', 'b', 'c'], [1, 2, 3]]
        }
        actual_disparate_header_info = InferTDRSchema(
            input_metadata=self.test_metadata_non_uniform,
            table_name=self.table_name,
            all_fields_non_required=False,
            allow_disparate_data_types_in_column=True  # allow disparate data types in column here
        )._check_type_consistency(key_value_mapping)

        expected_disparate_header_info = [
            {'header': 'column_a', 'force_to_string': False},
            {'header': 'column_b', 'force_to_string': True},
            {'header': 'column_c', 'force_to_string': True}
        ]
        self.assertEqual(actual_disparate_header_info, expected_disparate_header_info)

    def test_python_type_to_tdr_type_conversion_file_ref(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=["gs://bucket/some/file.txt"])
        self.assertEqual(res, "fileref")

    def test_python_type_to_tdr_type_conversion_boolean(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[True])
        self.assertEqual(res, "boolean")

    def test_python_type_to_tdr_type_conversion_list_of_booleans(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[[True], [True, False], [False]])
        self.assertEqual(res, "boolean")

    def test_python_type_to_tdr_type_conversion_ints(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[1.0, 2, 3.0])
        self.assertEqual(res, "int64")

    def test_python_type_to_tdr_type_conversion_floats(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[1.1, 2.2, 3.3])
        self.assertEqual(res, "float64")

    def test_python_type_to_tdr_type_conversion_float_and_ints(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[1.1, 2, 3.0])
        self.assertEqual(res, "float64")

    def test_python_type_to_tdr_type_conversion_list_of_ints(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[[1.0, 2], [3, 4.0], [5]])
        self.assertEqual(res, "int64")

    def test_python_type_to_tdr_type_conversion_list_of_floats(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[[1.0, 2.2], [3.4, 4.1], [5.9]])
        self.assertEqual(res, "float64")

    def test_python_type_to_tdr_type_conversion_list_of_floats_and_ints(self):
        res = self.infer_tdr_schema._python_type_to_tdr_type_conversion(values_for_header=[[1.0, 2], [3.4, 4.1], [5.0]])
        self.assertEqual(res, "float64")


def _old_style_strings():
    """
    Keep strings as plain python objects in DataFrames. pandas 3 loads strings into a dedicated dtype where
    missing values become NaN instead of None, which changes how the DataFrame based inference sees them.
    """
    try:
        return pd.option_context("future.infer_string", False)
    except (KeyError, ValueError, pd.errors.OptionError):
        return contextlib.nullcontext()


def _infer_both_ways(rows, **kwargs):
    """Infer the schema from a list (loaded into a DataFrame) and from an iterator (streamed)."""
    with _old_style_strings():
        from_dataframe = InferTDRSchema(input_metadata=list(rows), table_name="tbl", **kwargs).infer_schema()
    from_stream = InferTDRSchema(input_metadata=iter(rows), table_name="tbl", **kwargs).infer_schema()
    return from_dataframe, from_stream


class TestInferTDRSchemaStreaming:
    """The schema inferred from an iterator must match the one inferred from a list."""

    @pytest.mark.parametrize("allow_disparate", [True, False])
    @pytest.mark.parametrize("all_non_required", [True, False])
    def test_matches_dataframe_for_typical_tabular_data(self, allow_disparate, all_non_required):
        rows = [
            {"id": "a", "count": 1, "score": 1.5, "flag": True, "note": "x", "file": "gs://b/a.txt", "blank": None},
            {"id": "b", "count": 2, "score": 2.0, "flag": False, "note": "y", "file": "gs://b/b.txt", "blank": None},
            {"id": "c", "count": 3, "score": None, "flag": True, "note": "z", "file": "gs://b/c.txt", "blank": None},
        ]
        kwargs = {
            "allow_disparate_data_types_in_column": allow_disparate,
            "all_fields_non_required": all_non_required,
            "primary_key": "id",
        }
        from_dataframe, from_stream = _infer_both_ways(rows, **kwargs)
        assert from_stream == from_dataframe
        types = {c["name"]: c["datatype"] for c in from_stream["columns"] if "datatype" in c}
        assert types == {
            "id": "string", "count": "int64", "score": "float64", "flag": "boolean", "note": "string",
            "file": "fileref",
        }

    def test_expected_schema_for_a_stream(self):
        rows = (r for r in [{"a": "foo", "b": 100, "c": ["x", "y"]}, {"a": "bar", "b": 3030, "c": ["z"]}])
        schema = InferTDRSchema(input_metadata=rows, table_name="tbl").infer_schema()
        assert schema == {
            "name": "tbl",
            "columns": [
                {"name": "a", "required": True, "datatype": "string", "array_of": False},
                {"name": "b", "required": True, "datatype": "int64", "array_of": False},
                {"name": "c", "required": False, "datatype": "string", "array_of": True},
            ],
        }

    def test_stream_is_read_once_and_lazily(self):
        pulled = []

        def rows():
            for i in range(5):
                pulled.append(i)
                yield {"id": f"r{i}", "n": i}

        schema = InferTDRSchema(input_metadata=rows(), table_name="tbl").infer_schema()
        assert pulled == [0, 1, 2, 3, 4]
        assert [c["name"] for c in schema["columns"]] == ["id", "n"]

    def test_empty_input(self):
        assert InferTDRSchema(input_metadata=iter([]), table_name="tbl").infer_schema() == {
            "name": "tbl", "columns": []
        }

    def test_missing_keys_make_column_not_required(self):
        rows = [{"a": 1, "b": 1}, {"a": 2}, {"a": 3, "b": 3}]
        from_dataframe, from_stream = _infer_both_ways(rows)
        assert from_stream == from_dataframe
        assert [c["required"] for c in from_stream["columns"]] == [True, False]

    def test_mixed_types_are_an_error_unless_forced_to_string(self):
        rows = [{"a": "007"}, {"a": 5}]
        with pytest.raises(Exception, match="Not all values for the following headers are of the same type"):
            InferTDRSchema(input_metadata=iter(rows), table_name="tbl").infer_schema()
        schema = InferTDRSchema(
            input_metadata=iter(rows), table_name="tbl", allow_disparate_data_types_in_column=True
        ).infer_schema()
        assert schema["columns"][0]["datatype"] == "string"

    def test_forced_to_string_column_with_file_refs_stays_fileref(self):
        rows = [{"a": "gs://b/f.txt"}, {"a": 5}]
        from_dataframe, from_stream = _infer_both_ways(rows, allow_disparate_data_types_in_column=True)
        assert from_stream == from_dataframe
        assert from_stream["columns"][0]["datatype"] == "fileref"

    def test_all_falsy_column_is_a_string_but_not_if_it_has_nulls(self):
        # an all zero column is forced to a string. With nulls pandas loads it as floats where nulls are NaN
        rows = [{"zeros": 0, "zeros_with_null": 0}, {"zeros": 0, "zeros_with_null": None}]
        from_dataframe, from_stream = _infer_both_ways(rows)
        assert from_stream == from_dataframe
        types = {c["name"]: c["datatype"] for c in from_stream["columns"]}
        assert types == {"zeros": "string", "zeros_with_null": "int64"}

    def test_whole_numbers_stored_as_floats_are_ints(self):
        rows = [{"a": 1.0, "b": 1.0, "l": [1.0, 2.0]}, {"a": 2, "b": 2.5, "l": [3.0]}]
        from_dataframe, from_stream = _infer_both_ways(rows)
        assert from_stream == from_dataframe
        assert [c["datatype"] for c in from_stream["columns"]] == ["int64", "float64", "int64"]

    def test_empty_strings_are_ignored_for_data_types(self):
        rows = [{"a": 1, "b": ""}, {"a": "", "b": "  "}, {"a": 3, "b": ""}]
        from_dataframe, from_stream = _infer_both_ways(rows)
        assert from_stream == from_dataframe
        assert [(c["datatype"], c["required"]) for c in from_stream["columns"]] == [
            ("int64", True), ("string", True)
        ]

    def test_all_null_column_is_not_typed(self):
        rows = [{"a": 1, "b": None}, {"a": 2, "b": None}]
        from_dataframe, from_stream = _infer_both_ways(rows)
        assert from_stream == from_dataframe
        assert from_stream["columns"][1] == {"name": "b", "required": False, "data_type": "string"}

    def test_entity_prefix_is_removed_from_headers(self):
        rows = [{"entity:sample_id": "a", "entity_type": "x"}, {"entity:sample_id": "b", "entity_type": "y"}]
        from_dataframe, from_stream = _infer_both_ways(rows, primary_key="sample_id", all_fields_non_required=True)
        assert from_stream == from_dataframe
        assert [(c["name"], c["required"]) for c in from_stream["columns"]] == [
            ("sample_id", True), ("entity_type", False)
        ]

    def test_arrays(self):
        rows = [{"a": ["x"], "b": [1.5, 2.5], "c": [], "d": ["gs://b/f"]}, {"a": [], "b": [3.5], "c": [], "d": []}]
        from_dataframe, from_stream = _infer_both_ways(rows, allow_disparate_data_types_in_column=True)
        assert from_stream == from_dataframe
        assert [c["datatype"] for c in from_stream["columns"]] == ["string", "float64", "string", "fileref"]
        assert all(c["array_of"] and not c["required"] for c in from_stream["columns"])

    def test_first_array_empty_does_not_fail_when_forcing_strings(self):
        # the DataFrame based path raises an IndexError here
        rows = [{"a": []}, {"a": "x"}, {"a": ["y"]}]
        schema = InferTDRSchema(
            input_metadata=iter(rows), table_name="tbl", allow_disparate_data_types_in_column=True
        ).infer_schema()
        assert schema["columns"] == [{"name": "a", "required": False, "datatype": "string", "array_of": True}]

    def test_randomized_data_matches_dataframe(self):
        values = [
            None, float("nan"), "", "  ", "abc", "gs://b/f.txt", "007", 0, 1, -3, 2.0, 2.5, 0.0, True, False,
            [], [1, 2], [1.5], ["a"], ["gs://b/x"], [True],
        ]
        rng = random.Random(1234)
        for _ in range(400):
            columns = [f"c{i}" for i in range(rng.randint(1, 4))] + (["entity:id"] if rng.random() < 0.2 else [])
            palette = {c: rng.sample(values, rng.randint(1, 4)) for c in columns}
            rows = [
                {c: rng.choice(palette[c]) for c in columns if rng.random() < 0.9}
                for _ in range(rng.randint(0, 6))
            ]
            kwargs = {
                "all_fields_non_required": rng.random() < 0.3,
                "allow_disparate_data_types_in_column": rng.random() < 0.5,
                "primary_key": rng.choice([None, "c0", "id"]),
            }
            try:
                from_dataframe, from_stream = _infer_both_ways(rows, **kwargs)
            except IndexError:
                # the DataFrame based inference cannot handle a column whose first array is empty. Streaming can
                continue
            except Exception:
                # Both must reject the same data
                with pytest.raises(Exception):
                    InferTDRSchema(input_metadata=iter(rows), table_name="tbl", **kwargs).infer_schema()
                continue
            assert from_stream == from_dataframe, (rows, kwargs)
