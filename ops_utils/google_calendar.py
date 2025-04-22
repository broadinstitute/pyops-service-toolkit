from datetime import datetime, timedelta
from typing import List, Dict
from googleapiclient.discovery import build
from google.oauth2 import service_account


class GoogleCalendar:
    _SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

    def __init__(self, service_account_info: dict):
        """
        Initializes the GoogleCalendar instance using the user's credentials.

        **Args:**
        - service_account_info (dict): A dictionary containing the service account credentials.
        """
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=self._SCOPES
        )
        self._service = build(
            serviceName="calendar",
            version="v3",
            credentials=credentials,
            cache_discovery=False
        )

    @staticmethod
    def _create_calendar_string_from_datetime(dt: datetime) -> str:
        """
        Creates a string representation of a datetime object.

        **Args:**
        - dt (datetime): A datetime object.
        """
        return (datetime(dt.year, dt.month, dt.day, 00, 00)).isoformat() + 'Z'

    def get_events(self, calendar_id: str, days_back: int, days_ahead: int) -> List[Dict]:
        """
        Get all events from the specified calendar for the last `x` days.

        **Args:**
        - calendar_id (str): The ID of the Google Calendar.
        - days_back (int): Number of days in the past to retrieve events for.
        - days_ahead (int): Number of days in the future to retrieve events for.

        **Returns:**
        - list[dict]: A list of events with their details.
        """
        now = datetime.now()
        time_min = self._create_calendar_string_from_datetime(now - timedelta(days=days_back))
        time_max = self._create_calendar_string_from_datetime(now + timedelta(days=days_ahead))

        events_result = self._service.events().list(
            calendarId=calendar_id,
            timeMin=time_min,
            timeMax=time_max,
            maxResults=2500,
            singleEvents=True,
            orderBy='startTime').execute()
        # Return list of dict if results, otherwise empty list
        return events_result.get('items', [])
