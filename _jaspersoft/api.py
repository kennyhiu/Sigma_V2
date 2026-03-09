import requests
import sys
import time
from urllib.parse import quote
import base64

def paginate(url, headers):
    """Paginate through Jaspersoft resources using offset/limit query parameters.
    
    Jaspersoft uses query parameters (offset, limit) and the Next-Offset response
    header to control pagination, not a nextPageUrl field in the JSON response.
    """
    results = []
    limit = 100
    offset = 0
    
    while True:
        # add pagination params to the URL
        separator = "&" if "?" in url else "?"
        paginated_url = f"{url}{separator}offset={offset}&limit={limit}"
        
        response = requests.get(paginated_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # handle 204 No Content or empty response
        if response.status_code == 204 or not response.content:
            break
        
        try:
            data = response.json()
        except ValueError:  # includes JSONDecodeError
            # non-JSON response; stop pagination
            break
        
        # Jaspersoft returns an array of resourceLookup objects directly
        if isinstance(data, list):
            items = data
        else:
            # or sometimes wrapped in an object
            items = data.get("items", data.get("resourceLookup", []))
        print(f"Fetched {offset}, fetching another {len(items)} items from {paginated_url}", file=sys.stderr)
        results.extend(items)
        
        # check the Next-Offset header to see if there are more pages:
        # if it's absent, we've reached the last page
        next_offset = response.headers.get("Next-Offset")
        if next_offset is None:
            break
        
        offset = int(next_offset)
        time.sleep(0.1)  # Be polite and avoid hitting rate limits
    
    return results

class JaspersoftClient:
    def __init__(self, base_url, username, password):
        # try common option names (configparser lower-cases keys by default)
        self.base_url = base_url
        self.username = username
        self.password = password


    def _get_header(self):
        credentials = f"{self.username}:{self.password}"
        headers = {
            "Authorization": f"Basic {base64.b64encode(credentials.encode()).decode()}",
            "Accept": "application/json",
        }
        return headers


    def authenticate(self):
        # The login endpoint expects form fields, not Basic auth headers.  If
        # the credentials are incorrect or missing the server returns a 400.
        auth_url = f"{self.base_url.rstrip('/')}/rest_v2/login"
        data = {"j_username": self.username, "j_password": self.password}
        try:
            response = requests.post(auth_url, data=data, timeout=10)
            if response.status_code != 200:
                # include body so caller can see server message (often plain text)
                print(
                    f"Authentication failed ({response.status_code}): {response.text}",
                    file=sys.stderr,
                )
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            print(f"Authentication error: {e}", file=sys.stderr)
            return False


    def get_reports(self):
        # ensure we don't end up with a double-slash if base_url already
        # includes a trailing slash
        reports_url = f"{self.base_url.rstrip('/')}/rest_v2/resources"
        try:
            reports = paginate(reports_url, headers=self._get_header())
            return reports
        except requests.RequestException as e:
            print(f"Error fetching reports: {e}", file=sys.stderr)
            return []


    def get_scheduled_jobs(self):            
        scheduled_jobs_url = f"{self.base_url.rstrip('/')}/rest_v2/jobs"
        try:
            scheduled_jobs = paginate(scheduled_jobs_url, headers=self._get_header())
            return scheduled_jobs
        except requests.RequestException as e:
            print(f"Error fetching scheduled jobs: {e}", file=sys.stderr)
            return []


    def get_report_details(self, report_unit_uri):
        encoded_uri = quote(report_unit_uri, safe='/')
        report_url = f"{self.base_url}/rest_v2{encoded_uri}"
        try:
            headers = self._get_header()
            report_details = paginate(report_url, headers=headers)
            return report_details
        except requests.RequestException as e:
            print(f"Error fetching report details: {e}", file=sys.stderr)
            return None
    
    