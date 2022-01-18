"""REST client handling, including EmarsysStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

import base64, datetime, hashlib, uuid
from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream


class EmarsysStream(RESTStream):
    """Emarsys stream class."""

    url_base = "https://api.emarsys.net/api/v2"

    records_jsonpath = "$.data[*]"
    next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.

    @staticmethod
    def get_auth_header(username, secret):
        nonce = uuid.uuid4().hex
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        raw_password_digest = nonce + timestamp + secret
        encrypted_password_digest = hashlib.sha1()
        encrypted_password_digest.update(raw_password_digest.encode())
        pass_sha1 = encrypted_password_digest.hexdigest()
        pass_digest = base64.b64encode(pass_sha1.encode()).decode()
        header =  'UsernameToken Username="{}", PasswordDigest="{}", Nonce="{}", Created="{}"'.format(
            username,
            pass_digest,
            nonce,
            timestamp
        )
        return header

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["Content-Type"] = "application/json"
        headers["X-WSSE"] = self.get_auth_header(
            username=self.config["username"],
            secret=self.config["secret"]
        )
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # TODO: Delete this method if not needed.
        return row
