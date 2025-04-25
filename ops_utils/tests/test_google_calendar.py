from datetime import datetime
from unittest import TestCase
from unittest.mock import patch, MagicMock

from ops_utils.google_calendar import GoogleCalendar


class TestGoogleCalendar(TestCase):

    def setUp(self):
        # Patch credentials creation
        self.patcher_creds = patch("ops_utils.google_calendar.service_account.Credentials.from_service_account_info")
        self.mock_credentials_info = self.patcher_creds.start()

        # Patch API service build
        self.patcher_build = patch("ops_utils.google_calendar.build")
        self.mock_build = self.patcher_build.start()

        # Set return values
        self.fake_credentials = MagicMock()
        self.mock_credentials_info.return_value = self.fake_credentials

        self.fake_service = MagicMock()
        self.mock_build.return_value = self.fake_service

        # Create the GoogleCalendar instance
        self.fake_info = {"fake": "credentials"}
        self.google_calendar = GoogleCalendar(service_account_info=self.fake_info)

    def test_init_sets_up_service(self):
        """Test that the init method runs with the expected mocks"""
        self.mock_credentials_info.assert_called_once_with(
            self.fake_info, scopes=GoogleCalendar._SCOPES
        )
        self.mock_build.assert_called_once_with(
            serviceName="calendar", version="v3", credentials=self.fake_credentials, cache_discovery=False
        )
        self.assertEqual(self.google_calendar._service, self.fake_service)

    def test_create_calendar_string_from_datetime(self):
        """Test that the _create_calendar_string_from_datetime method returns the expected string"""
        # Set a static datetime for testing
        dt = datetime(2023, 10, 1, 12, 30)
        expected_result = "2023-10-01T00:00:00Z"

        # Call the method
        result = self.google_calendar._create_calendar_string_from_datetime(dt)

        # Assertions
        self.assertEqual(result, expected_result)

    @patch("ops_utils.google_calendar.GoogleCalendar._create_calendar_string_from_datetime")
    def test_get_events(self, mock_cal_string):
        # Mock the two calendar strings that get generated so we can actually use the time
        # stamps for testing
        mock_cal_string.side_effect = ["2023-01-01T00:00:00Z", "2023-01-10T00:00:00Z"]

        # Mock the return of events().list().execute()
        mock_events = MagicMock()
        self.fake_service.events.return_value = mock_events
        mock_list = MagicMock()
        mock_events.list.return_value = mock_list
        mock_list.execute.return_value = {"items": [{"id": "1", "summary": "Test Event"}]}

        # Call the method
        events = self.google_calendar.get_events(
            calendar_id="fake_calendar_id", days_back=5, days_ahead=5
        )

        # Assertions
        mock_events.list.assert_called_once_with(
            calendarId="fake_calendar_id",
            timeMin="2023-01-01T00:00:00Z",
            timeMax="2023-01-10T00:00:00Z",
            maxResults=2500,
            singleEvents=True,
            orderBy='startTime'
        )
        self.assertEqual(events, [{"id": "1", "summary": "Test Event"}])
