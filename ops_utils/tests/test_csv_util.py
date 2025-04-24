import pytest
from io import StringIO
from unittest.mock import patch

from ops_utils.csv_util import Csv


class TestCsv:

    @pytest.fixture(autouse=True)
    def setup(self):
        """
        Instantiate two instances of the Csv class, one with the delimiter
        as a comma, and one with the delimiter as a tab.
        """
        self.csv_comma_delimiter = Csv(
            file_path="fake_path.tsv", delimiter=","
        )
        self.csv_tab_delimiter = Csv(
            file_path="fake_path.tsv", delimiter="\t"
        )

    def test_create_tsv_from_list_of_dicts(self):
        """
        Test that creating a TSV from a list of dictionaries returns the
        expected file path AND the correct contents within the file that's generated
        """

        # Mock input data
        data = [{"columnA": "foo", "columnB": "bar"}, {"columnA": "baz", "columnB": "qux"}]
        expected_output = "columnA\tcolumnB\nfoo\tbar\nbaz\tqux\n"

        # Set up a StringIO to capture file writes
        mock_file = NonClosingStringIO()

        # Patch `open` so it returns our mock_file when called
        with patch("builtins.open", return_value=mock_file, create=True):
            path = self.csv_tab_delimiter.create_tsv_from_list_of_dicts(list_of_dicts=data)

        mock_file.seek(0)
        # Replace the difference in the newline characters so the outputs can be
        # correctly compared
        actual_output = mock_file.getvalue().replace('\r\n', '\n')
        assert actual_output == expected_output
        assert path == "fake_path.tsv"

    def test_create_tsv_from_list_of_dicts_headers_defined(self):
        """
        Test that creating a TSV from a list of dictionaries returns the
        expected file path AND the correct contents within the file that's generated.
        Also checks that the headers are written in the correct order (as
        provided in the input).
        """

        # Mock input data
        data = [{"columnA": "foo", "columnB": "bar"}, {"columnA": "baz", "columnB": "qux"}]
        expected_output = "columnB\tcolumnA\nbar\tfoo\nqux\tbaz\n"

        # Set up a StringIO to capture file writes
        mock_file = NonClosingStringIO()

        # Patch `open` so it returns our mock_file when called
        with patch("builtins.open", return_value=mock_file, create=True):
            path = self.csv_tab_delimiter.create_tsv_from_list_of_dicts(
                list_of_dicts=data, header_list=["columnB", "columnA"]
            )

        mock_file.seek(0)
        # Replace the difference in the newline characters so the outputs can be
        # correctly compared
        actual_output = mock_file.getvalue().replace('\r\n', '\n')
        assert actual_output == expected_output
        assert path == "fake_path.tsv"

    def test_create_tsv_from_list_of_lists_tab_delim(self):
        """
        Test that creating a TSV from a list of lists returns the
        expected file path AND assert that the correct contents get written to the file
        """

        data = [["foo", "bar"], ["baz", "qux"]]
        # Set up a StringIO to capture file writes
        mock_file = NonClosingStringIO()
        expected_output = "foo\tbar\nbaz\tqux\n"

        # Patch `open` so it returns our mock_file when called
        with patch("builtins.open", return_value=mock_file, create=True):
            path = self.csv_tab_delimiter.create_tsv_from_list_of_lists(list_of_lists=data)
        mock_file.seek(0)

        # Replace the difference in the newline characters so the outputs can be
        # correctly compared
        actual_output = mock_file.getvalue().replace('\r\n', '\n')
        assert expected_output == actual_output
        assert path == "fake_path.tsv"

    def test_create_tsv_from_list_of_lists_comma_delim(self):
        """
        Test that creating a TSV from a list of lists returns the
        expected file path AND assert the correct contents get written to the file
        """

        data = [["foo", "bar"], ["baz", "qux"]]
        # Set up a StringIO to capture file writes
        mock_file = NonClosingStringIO()
        expected_output = "foo,bar\nbaz,qux\n"

        # Patch `open` so it returns our mock_file when called
        with patch("builtins.open", return_value=mock_file, create=True):
            path = self.csv_comma_delimiter.create_tsv_from_list_of_lists(list_of_lists=data)
        mock_file.seek(0)

        # Replace the difference in the newline characters so the outputs can be
        # correctly compared
        actual_output = mock_file.getvalue().replace('\r\n', '\n')
        assert expected_output == actual_output
        assert path == "fake_path.tsv"

    def test_create_list_of_dicts_from_tsv_with_no_headers_tab_delim(self):
        """
        Test that creating a list of dictionaries from a TSV containing no headers
        returns the contents in the expected format
        """
        tsv_data = "foo\tbar\nbaz\tqux\n"

        # Expected header list
        headers = ["columnA", "columnB"]
        expected_output = [{"columnA": "foo", "columnB": "bar"}, {"columnA": "baz", "columnB": "qux"}]
        # Use StringIO to simulate the file
        mock_file = StringIO(tsv_data)

        # Patch `open` to return this fake file
        with patch("builtins.open", return_value=mock_file, create=True):
            result = self.csv_tab_delimiter.create_list_of_dicts_from_tsv_with_no_headers(headers)

        assert result == expected_output

    def test_create_list_of_dicts_from_tsv_with_no_headers_comma_delim(self):
        """
        Test that creating a list of dictionaries from a TSV containing no headers
        returns the contents in the expected format
        """
        csv_data = "foo,bar\nbaz,qux\n"

        # Expected header list
        headers = ["columnA", "columnB"]
        expected_output = [{"columnA": "foo", "columnB": "bar"}, {"columnA": "baz", "columnB": "qux"}]
        # Use StringIO to simulate the file
        mock_file = StringIO(csv_data)

        # Patch `open` to return this fake file
        with patch("builtins.open", return_value=mock_file, create=True):
            result = self.csv_comma_delimiter.create_list_of_dicts_from_tsv_with_no_headers(headers)

        assert result == expected_output

    def test_get_header_order_from_tsv_comma_delim(self):
        csv_data = "columnA,columnB\nbaz,qux\n"

        # Use StringIO to simulate the file
        mock_file = StringIO(csv_data)

        # Patch `open` to return this fake file
        with patch("builtins.open", return_value=mock_file, create=True):
            result = self.csv_comma_delimiter.get_header_order_from_tsv()
            assert result == ["columnA", "columnB"]

    def test_get_header_order_from_tsv_tab_delim(self):
        tsv_data = "columnA\tcolumnB\nfoo\tbar\n"
        mock_file = StringIO(tsv_data)
        with patch("builtins.open", return_value=mock_file, create=True):
            result = self.csv_tab_delimiter.get_header_order_from_tsv()
            assert result == ["columnA", "columnB"]

    def test_create_list_of_dicts_from_tsv_comma_delim(self):
        """
        Test that creating a list of dictionaries from an input
        CSV results in the correct value being returned
        """
        csv_content = "columnA,columnB,columnC\nfoo,bar,baz\n"

        with patch("builtins.open", return_value=StringIO(csv_content)):
            result = self.csv_comma_delimiter.create_list_of_dicts_from_tsv()

        assert result == [
            {
                "columnA": "foo",
                "columnB": "bar",
                "columnC": "baz",
            }
        ]

    def test_create_list_of_dicts_from_tsv_tab_delim(self):
        """
        Test that creating a list of dictionaries from an input
        TSV results in the correct value being returned
        """

        tsv_content = "columnA\tcolumnB\tcolumnC\nfoo\tbar\tbaz\n"

        with patch("builtins.open", return_value=StringIO(tsv_content)):
            result = self.csv_tab_delimiter.create_list_of_dicts_from_tsv()

        assert result == [
            {
                "columnA": "foo",
                "columnB": "bar",
                "columnC": "baz"
            }
        ]

    def test_create_list_of_dicts_from_tsv_tab_delim_wrong_expected_headers(self):
        """
        Test that a ValueError with the appropriate error message is raised when
        non-matching "expected" headers are provided
        """
        tsv_content = "columnA\tcolumnB\tcolumnC\nfoo\tbar\tbaz\n"

        with pytest.raises(ValueError, match="Expected headers not in fake_path.tsv"):
            with patch("builtins.open", return_value=StringIO(tsv_content)):
                self.csv_tab_delimiter.create_list_of_dicts_from_tsv(
                    expected_headers=["columnD", "columnE", "columnF"]
                )

class NonClosingStringIO(StringIO):
    """Class to prevent the file closing before the tests runs"""
    def close(self):
        pass
