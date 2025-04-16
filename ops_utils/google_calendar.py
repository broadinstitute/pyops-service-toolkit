from datetime import datetime, timedelta
from typing import List, Dict
from googleapiclient.discovery import build
from google.oauth2 import service_account


class GoogleCalendar:
    SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

    def __init__(self, service_account_info: dict):
        """
        Initialize the GoogleCalendar instance using the user's credentials.
        """
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=self.SCOPES
        )
        self.service = build(
            serviceName="calendar",
            version="v3",
            credentials=credentials,
            cache_discovery=False
        )

    def create_calendar_string_from_datetime(self, dt: datetime) -> str:
        return (datetime(dt.year, dt.month, dt.day, 00, 00)).isoformat() + 'Z'

    def get_events(self, calendar_id: str, days_back: int, days_ahead: int) -> List[Dict]:
        """
        Get all events from the specified calendar for the last `x` days.

        :param calendar_id: The ID of the Google Calendar.
        :param days_back: Number of days in the past to retrieve events for.
        :param days_ahead: Number of days in the future to retrieve events for.
        :return: A list of events with their details.
        """
        now = datetime.now()
        time_min = self.create_calendar_string_from_datetime(now - timedelta(days=days_back))
        time_max = self.create_calendar_string_from_datetime(now + timedelta(days=days_ahead))

        events_result = self.service.events().list(
            calendarId=calendar_id,
            timeMin=time_min,
            timeMax=time_max,
            maxResults=2500,
            singleEvents=True,
            orderBy='startTime').execute()
        # Return list of dict if results, otherwise empty list
        return events_result.get('items', [])
