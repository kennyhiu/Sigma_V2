import requests
from requests.auth import HTTPBasicAuth
from typing import Dict, List, Optional


class JaspersoftClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = True,
        timeout: int = 60,
    ):
        self.base_url = base_url.rstrip("/")
        self.rest_url = f"{self.base_url}/rest_v2"
        self.timeout = timeout

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.session.verify = verify_ssl
        self.session.headers.update({
            "Accept": "application/json"
        })

    def _raise_for_status(self, response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"HTTP {response.status_code} error for {response.request.method} {response.url}\n"
                f"Response: {response.text}"
            ) from exc

    def get_server_info(self) -> Dict:
        url = f"{self.rest_url}/serverInfo"
        response = self.session.get(url, timeout=self.timeout)
        self._raise_for_status(response)
        return response.json()

    def search_resources_page(
        self,
        folder_uri: str = "/",
        resource_type: Optional[str] = None,
        recursive: bool = True,
        limit: int = 100,
        offset: int = 0,
        expanded: bool = False,
        show_hidden_items: bool = False,
        force_full_page: bool = True,
    ) -> Dict:
        """
        Fetch one page of resources from JasperReports Server.
        """
        url = f"{self.rest_url}/resources"
        params = {
            "folderUri": folder_uri,
            "recursive": str(recursive).lower(),
            "limit": limit,
            "offset": offset,
            "expanded": str(expanded).lower(),
            "showHiddenItems": str(show_hidden_items).lower(),
            "forceFullPage": str(force_full_page).lower(),
        }

        if resource_type:
            params["type"] = resource_type

        response = self.session.get(url, params=params, timeout=self.timeout)
        self._raise_for_status(response)

        data = response.json()

        return {
            "data": data,
            "headers": dict(response.headers),
            "status_code": response.status_code,
            "url": response.url,
        }

    @staticmethod
    def _extract_items(payload) -> List[Dict]:
        """
        Jaspersoft responses are usually a list or wrapped structure depending on endpoint/version.
        This helper tries to normalize them.
        """
        if isinstance(payload, list):
            return payload

        if isinstance(payload, dict):
            # common possible wrappers
            for key in ("resourceLookup", "resources", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    return value

        return []

    def get_all_reports(
        self,
        folder_uri: str = "/",
        page_size: int = 100,
        recursive: bool = True,
        expanded: bool = False,
        show_hidden_items: bool = False,
        verbose: bool = True,
        exclude_patterns: Optional[List] = None,  # list of compiled regex patterns
    ) -> List[Dict]:
        """
        Progressively fetch all reportUnit resources until no more pages remain.
        Optionally exclude reports whose URI match any of the exclude_patterns (regex).
        """
        if exclude_patterns is None:
            exclude_patterns = []

        all_reports: List[Dict] = []
        offset = 0
        page_num = 1

        while True:
            result = self.search_resources_page(
                folder_uri=folder_uri,
                resource_type="reportUnit",
                recursive=recursive,
                limit=page_size,
                offset=offset,
                expanded=expanded,
                show_hidden_items=show_hidden_items,
                force_full_page=True,
            )
            items = self._extract_items(result["data"])
            headers = result["headers"]

            # Filter items based on exclude patterns
            # if patterns were compiled regexes use them, otherwise fall back to substring
            filtered_items = []
            for item in items:
                uri = item.get("uri", "")
                skip = False
                for pattern in exclude_patterns:
                    # Debug: print pattern and uri being matched
                    if hasattr(pattern, "search"):
                        match = pattern.search(uri)
                        if match:
                            print(f"DEBUG: Excluded by regex {pattern.pattern}: {uri}", file=__import__('sys').stderr)
                            skip = True
                            break
                    else:
                        if pattern in uri:
                            print(f"DEBUG: Excluded by substring {pattern}: {uri}", file=__import__('sys').stderr)
                            skip = True
                            break
                if not skip:
                    filtered_items.append(item)

            if verbose:
                print(
                    f"Page {page_num}: fetched {len(items)} reports, "
                    f"filtered to {len(filtered_items)} "
                    f"(offset={offset}, next_offset={headers.get('Next-Offset')})"
                )

            all_reports.extend(filtered_items)

            next_offset = headers.get("Next-Offset")
            if not next_offset:
                break

            try:
                offset = int(next_offset)
            except ValueError:
                break

            page_num += 1

        return all_reports

    