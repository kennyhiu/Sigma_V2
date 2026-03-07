import requests
import sys
import time


def _pick_cursor_param(url, cursor_value):
    # /v2/workbooks/<id>/version-history behaves like page-number pagination.
    if "/v2/workbooks/" in url and "/version-history" in url:
        return "page"
    if "/v2.1/" in url:
        return "page"
    return "page" if cursor_value.isdigit() else "nextPage"


def paginate(url, headers, params=None, timeout=30, max_pages=1000):
    results = []
    next_cursor = None
    cursor_param = None
    page_num = 0
    seen_cursors = set()
    fallback_attempts = set()
    params = {} if params is None else params.copy()
    while True:
        page_num += 1
        if page_num > max_pages:
            print(f"{url} [paginate] stopping at max_pages={max_pages}")
            break

        if next_cursor and cursor_param:
            # Prevent accidental infinite loops when API returns same cursor repeatedly.
            cursor_key = (cursor_param, next_cursor)
            if cursor_key in seen_cursors:
                if cursor_param in {"nextPage", "page"}:
                    alternate = "page" if cursor_param == "nextPage" else "nextPage"
                    alt_key = (alternate, next_cursor)
                    fallback_key = (cursor_param, alternate, next_cursor)
                    if alt_key not in seen_cursors and fallback_key not in fallback_attempts:
                        fallback_attempts.add(fallback_key)
                        cursor_param = alternate
                        # Silent one-time fallback between nextPage/page to avoid noisy logs.
                        cursor_key = (cursor_param, next_cursor)
                    else:
                        print(
                            f"{url} [paginate] repeated cursor detected "
                            f"param={cursor_key[0]} value={cursor_key[1]}; stopping."
                        )
                        break
                else:
                    print(
                        f"{url} [paginate] repeated cursor detected "
                        f"param={cursor_param} value={next_cursor}; stopping."
                    )
                    break
            seen_cursors.add(cursor_key)
            if cursor_param == "page":
                params.pop("nextPage", None)
            elif cursor_param == "nextPage":
                params.pop("page", None)
            params[cursor_param] = next_cursor

        r = None
        for attempt in range(4):
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code != 429:
                break
            retry_after_raw = r.headers.get("Retry-After", "1")
            try:
                wait_seconds = max(1, int(float(retry_after_raw)))
            except ValueError:
                wait_seconds = 1
            print(f"{url} [paginate] 429 rate limited; retrying in {wait_seconds}s (attempt {attempt + 1}/4)")
            time.sleep(wait_seconds)
        if r is None:
            raise RuntimeError("Request did not execute.")
        r.raise_for_status()
        payload = r.json()

        if isinstance(payload, list):
            entries = payload
            next_cursor = None
            cursor_param = None
        elif isinstance(payload, dict):
            entries = payload.get("entries", [])
            if not isinstance(entries, list):
                entries = []
            token = payload.get("nextPageToken")
            if isinstance(token, str) and token.strip():
                next_cursor = token.strip()
                cursor_param = "nextPageToken"
                params.pop("nextPage", None)
            else:
                next_page = payload.get("nextPage")
                if isinstance(next_page, str) and next_page.strip():
                    next_cursor = next_page.strip()
                    cursor_param = _pick_cursor_param(url, next_cursor)
                    params.pop("nextPageToken", None)
                elif isinstance(next_page, dict):
                    token = next_page.get("token") or next_page.get("nextPageToken")
                    if isinstance(token, str) and token.strip():
                        next_cursor = token.strip()
                        cursor_param = "nextPageToken"
                        params.pop("nextPage", None)
                    else:
                        next_cursor = None
                        cursor_param = None
                else:
                    next_cursor = None
                    cursor_param = None
            if payload.get("hasMore") and not next_cursor:
                print(f"{url} [paginate] hasMore=true but missing next cursor; stopping.")
        else:
            entries = []
            next_cursor = None
            cursor_param = None

        results.extend(entries)
        #print(
        #    f"{url} [paginate] page={page_num} rows={len(entries)} total={len(results)} "
        #    f"next={'yes' if bool(next_cursor) else 'no'} "
        #    f"cursor_param={cursor_param or '-'}"
        #)

        if not next_cursor:
            break

    return results
 
class SigmaAPI:
    def __init__(self, base_url, client_id, client_secret):
        # try common option names (configparser lower-cases keys by default)
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        # Lazy token retrieval: do not throw on init if config is incomplete
        self.token = None
        self._tag_name_by_id = {}
        self._tags_loaded = False

    def get_access_token(self):
        if self.token:
            return self.token
        url = f"{self.base_url.rstrip('/')}/v2/auth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        try:
            # The token endpoint expects form-encoded data, not JSON.
            headers = {"accept": "application/json",
                       "Content-Type": "application/x-www-form-urlencoded"}
            response = requests.post(url, data=payload, headers=headers, timeout=10)
            response.raise_for_status()
            # accept common key names
            data = response.json()
            self.token = data.get('access_token') or data.get('token') or data.get('id_token')
            if self.token:
                return self.token
        except Exception as exc:
            print(exc)
            sys.exit(1)

    def get_headers(self):
        token = self.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
          
    def authenticate(self):
        token = self.get_access_token()
        return token is not None
    
    def get_all_teams(self):      
        url = f"{self.base_url.rstrip('/')}/v2.1/teams"
        headers = self.get_headers()
        try:        
            teams = paginate(url, headers)
            return teams
        except Exception as exc:
            print(exc)
            return None

    def get_member_details(self, member_id):
        url = f"{self.base_url.rstrip('/')}/v2/members/{member_id}"
        headers = self.get_headers()
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            print(exc)
            return None

    def get_all_members(self):      
        url = f"{self.base_url.rstrip('/')}/v2.1/members?includeArchived=false&includeInactive=false"
        headers = self.get_headers()
        try:        
            members = paginate(url, headers)
            return members
        except Exception as exc:
            print(exc)
            return None
        
    def get_all_workbooks(self):      
        url = f"{self.base_url.rstrip('/')}/v2/workbooks"
        headers = self.get_headers()
        try:        
            workbooks = paginate(url, headers)
            return workbooks
        except Exception as exc:
            print(exc)
            return None
        
    def get_workbook_tags(self,workbook_urlid):      
        url = f"{self.base_url.rstrip('/')}/v2/workbooks/{workbook_urlid}/tags"
        headers = self.get_headers()
        try:        
            tags = paginate(url, headers)
            return tags
        except Exception as exc:
            print(exc)
            return None
    
    def get_tag_name(self, tag_id):
        if not tag_id:
            return None
        if tag_id in self._tag_name_by_id:
            return self._tag_name_by_id.get(tag_id)

        if not self._tags_loaded:
            url = f"{self.base_url.rstrip('/')}/v2/tags"
            headers = self.get_headers()
            try:
                tags = paginate(url, headers)
                for tag in tags:
                    if not isinstance(tag, dict):
                        continue
                    tag_key = tag.get("versionTagId") or tag.get("tagId") or tag.get("id")
                    if not tag_key:
                        continue
                    self._tag_name_by_id[tag_key] = tag.get("name")
                self._tags_loaded = True
            except Exception as exc:
                print(exc)
                return None

        return self._tag_name_by_id.get(tag_id)
    
    def get_workbook_version_history(self,workbook_urlid):      
        url = f"{self.base_url.rstrip('/')}/v2/workbooks/{workbook_urlid}/version-history"
        headers = self.get_headers()
        try:        
            revisions = paginate(url, headers)
            return revisions
        except Exception as exc:
            print(exc)
            return None
