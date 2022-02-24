"""Stream type classes for tap-emarsys."""
import copy
import datetime
from urllib.parse import urlparse
from urllib.parse import parse_qs
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_emarsys.client import EmarsysStream


class FieldsStream(EmarsysStream):
    name = "fields"
    path = "/field/translate/{language_id}"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("application_type", th.StringType),
        th.Property("string_id", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "field_id": record["id"],
            "field_string_id": record["string_id"]
        }


class ContactListsStream(EmarsysStream):
    name = "contact_lists"
    path = "/contactlist"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("type", th.NumberType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "contact_list_id": record["id"]
        }


# TODO: Can this also take fields?
class ContactListContactsStream(EmarsysStream):
    name = "contact_list_contacts"
    parent_stream_type = ContactListsStream
    ignore_parent_replication_keys = True
    path = "/contactlist/{contact_list_id}/contacts/data"
    primary_keys = ["contact_id", "contact_list_id"]
    records_jsonpath = "$.data[*]..fields"
    next_page_token_jsonpath = None

    schema = th.PropertiesList(
        th.Property("contact_id", th.NumberType),
        th.Property("uid", th.StringType),
        th.Property("contact_list_id", th.NumberType),
    ).to_dict()
    
    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["contact_id"] = row["id"]
        return row

    
class ContactFieldsStream(EmarsysStream):
    name = "contact_fields"
    parent_stream_type = FieldsStream
    ignore_parent_replication_keys = True
    path = "/contact/query/?return={field_id}"
    primary_keys = ["contact_id", "field_id"]
    records_jsonpath = "$.data.result[*]"
    next_page_token_jsonpath = None
    replication_key = None

    schema = th.PropertiesList(
        th.Property("contact_id", th.NumberType),
        th.Property("field_id", th.NumberType),
        th.Property("field_string_id", th.StringType),
        th.Property("value", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        return {
            "contact_id": row["id"],
            "field_id": context["field_id"],
            "field_string_id": context["field_string_id"],
            "value": row[str(context["field_id"])]
        }
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        params = {
            'limit': 10000,
            'offset': next_page_token if next_page_token else 0,
            'excludeempty': True
        }
        self.logger.debug(params)
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        elif response.headers.get("X-Next-Page", None):
            next_page_token = response.headers.get("X-Next-Page", None)
        else:
            offset = int(parse_qs(urlparse(response.request.url).query)["offset"][0])
            if len(response.json()["data"]["result"]) > 0:
                next_page_token = offset + 10000
            else:
                next_page_token = None
        return next_page_token


class SegmentIdsStream(EmarsysStream):
    name = "segment_ids"
    path = "/filter"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("predefinedSegmentId", th.NumberType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "segment_id": record["id"]
        }


class SegmentStream(EmarsysStream):
    name = "segments"
    parent_stream_type = SegmentIdsStream
    ignore_parent_replication_keys = True
    path = "/filter/{segment_id}"
    primary_keys = ["id"]
    next_page_token_jsonpath = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("type", th.StringType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("base_contact_list_id", th.NumberType),
        th.Property("criteria_types", th.ArrayType(th.StringType)),
    ).to_dict()


class EmailCampaignsStream(EmarsysStream):
    name = "email_campaigns"
    path = "/email"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("language", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("deleted", th.DateTimeType),
        th.Property("fromemail", th.StringType),
        th.Property("name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("api_status", th.NumberType),
        th.Property("api_error", th.NumberType),
        th.Property("event_id", th.StringType),
        th.Property("is_delayed", th.NumberType),
        th.Property("administrator_id", th.StringType),
        th.Property("force_attachment", th.BooleanType),
        th.Property("is_rti", th.BooleanType),
        th.Property("fromname", th.StringType),
        th.Property("subject", th.StringType),
        th.Property("email_category", th.StringType),
        th.Property("filter", th.NumberType),
        th.Property("exclude_filter", th.NumberType),
        th.Property("contactlist", th.StringType),
        th.Property("exclude_contactlist", th.NumberType),
        th.Property("template", th.StringType),
        th.Property("unsubscribe", th.StringType),
        th.Property("browse", th.StringType),
        th.Property("text_only", th.StringType),
        th.Property("source", th.StringType),
        th.Property("content_type", th.StringType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("features", th.ArrayType(th.StringType)),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "email_campaign_id": record["id"],
            "email_created_at": record["created"],
            "email_deleted_at": record["deleted"],
            "email_status": record["status"],
        }
    
    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["id"] = int(row["id"])
        row["filter"] = int(row["filter"])
        row["exclude_filter"] = int(row["exclude_filter"])
        row["api_error"] = int(row["api_error"])
        row["api_status"] = int(row["api_status"])
        return row


class EmailCampaignDetailsStream(EmarsysStream):
    name = "email_campaign_details"
    parent_stream_type = EmailCampaignsStream
    ignore_parent_replication_keys = True
    path = "/email/{email_campaign_id}/"
    primary_keys = ["id"]
    next_page_token_jsonpath = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("additional_linktracking_parameters", th.StringType),
    ).to_dict()


class EmailCategoriesStream(EmarsysStream):
    name = "email_categories"
    path = "/emailcategory"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("category", th.StringType),
    ).to_dict()

    
class EmailResponseSummariesStream(EmarsysStream):
    name = "email_response_summaries"
    parent_stream_type = EmailCampaignsStream
    ignore_parent_replication_keys = True
    path = "/email/{email_campaign_id}/responsesummary/"
    primary_keys = ["email_campaign_id", "date"]
    replication_key = "date"
    next_page_token_jsonpath = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("email_campaign_id", th.NumberType),
        th.Property("date", th.DateTimeType),
        th.Property("sent", th.NumberType),
        th.Property("planned", th.NumberType),
        th.Property("soft_bounces", th.NumberType),
        th.Property("hard_bounces", th.NumberType),
        th.Property("block_bounces", th.NumberType),
        th.Property("opened", th.NumberType),
        th.Property("unsubscribe", th.NumberType),
        th.Property("total_clicks", th.NumberType),
        th.Property("unique_clicks", th.NumberType),
        th.Property("complained", th.NumberType),
        th.Property("launches", th.NumberType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        if isinstance(next_page_token, datetime.datetime):
            start_date = next_page_token
        else:
            start_date = datetime.datetime.strptime(context["email_created_at"], "%Y-%m-%d %H:%M:%S")
        deleted_date = datetime.datetime.strptime(context["email_deleted_at"], "%Y-%m-%d %H:%M:%S") if context["email_deleted_at"] else None
        today = datetime.datetime.now(tz=start_date.tzinfo)
        final_date = deleted_date if deleted_date else today
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': (
                min(start_date + datetime.timedelta(days=1), final_date)
            ).strftime('%Y-%m-%d'),
        }
        context["start_date"] = start_date.strftime('%Y-%m-%d')
        self.logger.debug(params)
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        elif response.headers.get("X-Next-Page", None):
            next_page_token = response.headers.get("X-Next-Page", None)
        else:
            start_date = datetime.datetime.strptime(
                parse_qs(urlparse(response.request.url).query)["start_date"][0],
                "%Y-%m-%d"
            )
            end_date = datetime.datetime.strptime(
                parse_qs(urlparse(response.request.url).query)["end_date"][0],
                "%Y-%m-%d"
            )
            if start_date.date() < end_date.date():
                next_page_token = end_date
            else:
                next_page_token = None
        return next_page_token

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["date"] = context["start_date"]
        row["email_campaign_id"] = int(context["email_campaign_id"])
        row["planned"] = int(row["planned"])
        row["total_clicks"] = int(row["total_clicks"])
        row["unique_clicks"] = int(row["unique_clicks"])
        row["sent"] = int(row["sent"])
        row["soft_bounces"] = int(row["soft_bounces"])
        row["hard_bounces"] = int(row["hard_bounces"])
        row["block_bounces"] = int(row["block_bounces"])
        row["opened"] = int(row["opened"])
        row["unsubscribe"] = int(row["unsubscribe"])
        row["complained"] = int(row["complained"])
        row["launches"] = int(row["launches"])
        return row

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.
        Each row emitted should be a dictionary of property names to their values.
        """
        if context["email_status"] in ('1', '4'):
            self.logger.debug("Skipping campaign {campaign_id} sync.".format(campaign_id=context["email_campaign_id"]))
            return []
        return super().get_records(context)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        By default, checks for error status codes (>400) and raises a
        :class:`singer_sdk.exceptions.FatalAPIError`.

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png


        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if response.status_code == 401:
            print(response.content)
            print(response.request.url)
            print(response.request.headers)
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            self._requests_session = None
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise FatalAPIError(msg)
        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.
        If pagination is detected, pages will be recursed automatically.
        Args:
            context: Stream partition or context dictionary.
        Yields:
            An item for every record in the response.
        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            try:
                resp = decorated_request(prepared_request, context)
            except RetriableAPIError as e:
                print(e)
                return self.request_records(context=context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token



class EmailCampaignTrackedLinksStream(EmarsysStream):
    name = "email_campaign_tracked_links"
    parent_stream_type = EmailCampaignsStream
    ignore_parent_replication_keys = True
    path = "/email/{email_campaign_id}/trackedlinks/"
    primary_keys = ["email_campaign_id", "id"]
    next_page_token_jsonpath = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("section_id", th.NumberType),
        th.Property("email_campaign_id", th.NumberType),
        th.Property("url", th.StringType),
        th.Property("tracked_url", th.StringType),
    ).to_dict()

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        By default, checks for error status codes (>400) and raises a
        :class:`singer_sdk.exceptions.FatalAPIError`.

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png


        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        if response.status_code == 401:
            print(response.content)
            print(response.request.url)
            print(response.request.headers)
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            self._requests_session = None
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise FatalAPIError(msg)
        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)
