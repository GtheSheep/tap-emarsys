"""Emarsys tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_emarsys.streams import (
    FieldsStream,
    ContactIdsStream,
    ContactFieldsStream,
    ContactListsStream,
    ContactListContactsStream,
    SegmentIdsStream,
    SegmentStream,
    EmailCampaignsStream,
    EmailCampaignTrackedLinksStream,
    EmailCampaignDetailsStream,
    EmailCategoriesStream,
    EmailResponseSummariesStream,
)

STREAM_TYPES = [
    FieldsStream,
    ContactIdsStream,
    ContactFieldsStream,
    ContactListsStream,
    ContactListContactsStream,
    SegmentIdsStream,
    SegmentStream,
    EmailCampaignsStream,
    EmailCampaignTrackedLinksStream,
    EmailCampaignDetailsStream,
    EmailCategoriesStream,
    EmailResponseSummariesStream
]


class TapEmarsys(Tap):
    name = "tap-emarsys"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="The username obtained for the API user"
        ),
        th.Property(
            "secret",
            th.StringType,
            required=True,
            description="The secret obtained for the API user"
        ),
        th.Property(
            "language_id",
            th.StringType,
            default="en",
            description="The language ID for data to be retrieved in"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
