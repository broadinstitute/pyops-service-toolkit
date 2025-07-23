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