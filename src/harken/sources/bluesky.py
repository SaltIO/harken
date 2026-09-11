"""Bluesky search, optionally authenticated through a Bluesky-hosted account's PDS."""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from harken.models import Mention
from harken.sources.base import FetchPage, Source, retry_after_seconds

# Public AppView search can return 403 even when public profile lookup works;
# configured accounts use authenticated PDS proxying instead.
_API = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"
_LOGIN = "https://bsky.social/xrpc/com.atproto.server.createSession"


class BlueskySource(Source):
    name = "bluesky"
    label = "Bluesky"
    needs_config = False

    def __init__(self, **options):
        super().__init__(**options)
        if bool(options.get("handle")) != bool(options.get("app_password")):
            raise ValueError("Bluesky handle and app password must be set together")
        self._auth_cache = options.get("batch_cache")
        if self._auth_cache is None:
            self._auth_cache = {}

    def _authenticate(self, client: httpx.Client) -> tuple[str, str]:
        if self._auth_cache.get("error"):
            raise RuntimeError(self._auth_cache["error"])
        if self._auth_cache.get("session"):
            return self._auth_cache["session"]
        # Cache failures as well: one password login attempt per source instance,
        # even when a multi-term batch or the pipeline retries a failed fetch.
        self._auth_cache["error"] = "Bluesky login failed; check the handle and app password"
        try:
            response = client.post(
                _LOGIN,
                json={"identifier": self.options["handle"],
                      "password": self.options["app_password"]},
                follow_redirects=False,
            )
            if response.status_code != 200:
                retry = retry_after_seconds(response.headers.get("Retry-After"))
                marker = f" retry_after_seconds={retry}" if retry is not None else ""
                self._auth_cache["error"] = (
                    f"Bluesky login failed (HTTP {response.status_code}){marker}"
                )
                raise RuntimeError(self._auth_cache["error"])
            data = response.json()
            token = data["accessJwt"]
            if not isinstance(token, str) or not token or any(
                ord(char) < 33 or ord(char) > 126 for char in token
            ):
                raise ValueError("Invalid session token")
            did = data["did"]
            doc = data["didDoc"]
            if not isinstance(did, str) or not did.startswith("did:") or doc["id"] != did:
                raise ValueError("Invalid session identity")
            endpoints = [
                service["serviceEndpoint"] for service in doc["service"]
                if service.get("id") in {"#atproto_pds", f"{did}#atproto_pds"}
                and service.get("type") == "AtprotoPersonalDataServer"
            ]
            if len(endpoints) != 1:
                raise ValueError("Invalid PDS service")
            endpoint = endpoints[0]
            if not isinstance(endpoint, str) or any(
                ord(char) < 33 or ord(char) > 126 for char in endpoint
            ):
                raise ValueError("Invalid PDS URL")
            url = urlsplit(endpoint)
            host = url.hostname or ""
            if (url.scheme != "https" or url.username or url.password
                    or url.port not in {None, 443} or url.path not in {"", "/"}
                    or url.query or url.fragment
                    or not (host == "bsky.social" or host.endswith(".bsky.social")
                            or host.endswith(".host.bsky.network"))):
                raise ValueError("Unsupported PDS URL")
            self._auth_cache["session"] = (endpoint.rstrip("/"), token)
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            raise RuntimeError(self._auth_cache["error"]) from None
        self._auth_cache["error"] = None
        return self._auth_cache["session"]

    def fetch(self, query: str, limit: int = 50) -> list[Mention]:
        return self.fetch_page(query, limit=limit).mentions

    def fetch_page(
        self,
        query: str,
        limit: int = 50,
        *,
        cursor: str | None = None,
        since: datetime | None = None,
    ) -> FetchPage:
        params = {"q": query, "limit": min(limit, 100), "sort": "latest"}
        if cursor:
            params["cursor"] = cursor
        if since:
            params["since"] = since.isoformat().replace("+00:00", "Z")
        with self._client() as client:
            api = _API
            headers = {}
            if self.options.get("handle"):
                pds, token = self._authenticate(client)
                api = f"{pds}/xrpc/app.bsky.feed.searchPosts"
                headers = {"Authorization": f"Bearer {token}",
                           "atproto-proxy": "did:web:api.bsky.app#bsky_appview"}
            try:
                resp = client.get(api, params=params, headers=headers, follow_redirects=False)
            except httpx.HTTPError:
                raise RuntimeError("Bluesky search transport failed") from None
            if not resp.is_success:
                # Do not retain bearer headers or a server-controlled error body
                # in an exception. Keep status/Retry-After for pipeline backoff.
                request = httpx.Request("GET", _API)
                retry = retry_after_seconds(resp.headers.get("Retry-After"))
                marker = f" retry_after_seconds={retry}" if retry is not None else ""
                response = httpx.Response(
                    resp.status_code, request=request,
                    headers={"Retry-After": str(retry)} if retry is not None else {},
                )
                raise httpx.HTTPStatusError(
                    f"Bluesky search failed (HTTP {resp.status_code}){marker}",
                    request=request, response=response,
                ) from None
            data = resp.json()

        mentions: list[Mention] = []
        for post in data.get("posts", []):
            author = post.get("author", {})
            record = post.get("record", {})
            handle = author.get("handle")
            uri = post.get("uri", "")
            rkey = uri.split("/")[-1] if uri else ""
            published = _parse(record.get("createdAt"))
            indexed = _parse(post.get("indexedAt"))
            did = author.get("did")
            native_id = uri if isinstance(uri, str) and uri.startswith("at://") else None
            mentions.append(
                Mention(
                    source=self.name,
                    source_item_id=native_id,
                    author_id=did,
                    query=query,
                    author=handle,
                    title=None,
                    text=record.get("text", ""),
                    url=f"https://bsky.app/profile/{did or handle}/post/{rkey}"
                    if (did or handle) and rkey
                    else None,
                    created_at=published or indexed or datetime.now(timezone.utc),
                    published_at=published,
                    publication_provenance="record.createdAt" if published else "unknown",
                    indexed_at=indexed,
                    score=post.get("likeCount"),
                )
            )
        return FetchPage(mentions, data.get("cursor"), truncated=bool(data.get("cursor")))


def _parse(s: str | None) -> datetime | None:
    if not isinstance(s, str) or not s:
        return None
    try:
        value = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return value.astimezone(timezone.utc) if value.tzinfo else None
    except ValueError:
        return None
