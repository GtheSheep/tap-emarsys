"""Stream type classes for tap-emarsys."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

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
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]..fields"
    next_page_token_jsonpath = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("uid", th.StringType),
        th.Property("contact_list_id", th.NumberType),
    ).to_dict()


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
        th.Property("api_status", th.StringType),
        th.Property("api_error", th.StringType),
        th.Property("event_id", th.StringType),
        th.Property("is_delayed", th.NumberType),
        th.Property("administrator_id", th.StringType),
        th.Property("force_attachment", th.BooleanType),
        th.Property("is_rti", th.BooleanType),
        th.Property("fromname", th.StringType),
        th.Property("subject", th.StringType),
        th.Property("email_category", th.StringType),
        th.Property("filter", th.StringType),
        th.Property("exclude_filter", th.StringType),
        th.Property("contactlist", th.StringType),
        th.Property("exclude_contactlist", th.StringType),
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
            "email_campaign_id": record["id"]
        }


class EmailCategoriesStream(EmarsysStream):
    name = "email_categories"
    path = "/emailcategory"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("category", th.StringType),
    ).to_dict()
