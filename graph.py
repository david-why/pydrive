import time
import typing as t
from datetime import datetime, timedelta
import urllib.parse

from requests import Session
from requests.auth import AuthBase
from requests.models import PreparedRequest


class NoRefreshTokenError(ValueError):
    pass


class APIError(ValueError):
    pass


class AuthoizationTimeoutError(TimeoutError):
    pass


class AuthorizationCanceledError(RuntimeError):
    pass


class GraphAuth(AuthBase):
    def __init__(
        self,
        client_id: str,
        scopes: list[str],
        access_token: str | None = None,
        expires: datetime | None = None,
        refresh_token: str | None = None,
        tenant: str = 'organizations',
        session: Session | None = None,
    ) -> None:
        if access_token is None and refresh_token is None:
            raise NoRefreshTokenError('No refresh token or access token')
        self.client_id = client_id
        self.scopes = scopes
        self._access_token = access_token
        self.expires = expires or datetime.utcnow()
        self.refresh_token = refresh_token
        self.tenant = tenant
        self.session = session or Session()
        if access_token is None:
            self.refresh()

    @property
    def access_token(self) -> str:
        if datetime.utcnow() > self.expires or self._access_token is None:
            self.refresh()
        return self._access_token  # type: ignore

    def refresh(self) -> str:
        if self.refresh_token is None:
            raise NoRefreshTokenError('No refresh token')
        r = self.session.post(
            'https://login.microsoftonline.com/%s/oauth2/v2.0/token' % self.tenant,
            data={
                'client_id': self.client_id,
                'grant_type': 'refresh_token',
                'scope': ' '.join(self.scopes),
                'refresh_token': self.refresh_token,
            },
        ).json()
        if 'error' in r:
            raise APIError(r)
        self.expires = datetime.utcnow() + timedelta(seconds=r['expires_in'])
        self.refresh_token = r['refresh_token']
        self._access_token = r['access_token']
        return self._access_token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        if 'Authorization' not in r.headers:
            r.headers['Authorization'] = 'Bearer ' + self.access_token
        return r


class GraphAPI:
    def __init__(
        self,
        client_id: str,
        scopes: list[str],
        tenant: str = 'organizations',
        refresh_token: str | None = None,
    ) -> None:
        self.client_id = client_id
        self.scopes = scopes
        self.tenant = tenant
        self.session = Session()
        if refresh_token is not None:
            self.session.auth = GraphAuth(
                client_id, scopes, refresh_token=refresh_token, tenant=tenant
            )

    def authenticate(
        self, callback: t.Callable[[str, str, datetime], None] | None = None
    ):
        session = Session()
        r = session.post(
            'https://login.microsoftonline.com/%s/oauth2/v2.0/devicecode' % self.tenant,
            data={'client_id': self.client_id, 'scope': ' '.join(self.scopes)},
        ).json()
        if 'error' in r:
            raise APIError(r)
        expires = datetime.utcnow() + timedelta(seconds=r['expires_in'])
        if callback:
            callback(r['user_code'], r['verification_uri'], expires)
        else:
            print(r['message'])
        device_code = r['device_code']
        interval = r['interval']
        data = None
        while datetime.utcnow() < expires:
            r = session.post(
                'https://login.microsoftonline.com/%s/oauth2/v2.0/token' % self.tenant,
                data={
                    'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                    'client_id': self.client_id,
                    'device_code': device_code,
                },
            ).json()
            if 'error' not in r:
                data = r
                break
            if r['error'] == 'authorization_pending':
                time.sleep(interval)
                continue
            if r['error'] == 'authorization_pending':
                raise AuthorizationCanceledError('User canceled authorization')
            raise APIError(r)
        if data is None:
            raise AuthoizationTimeoutError('User authorization timed out')
        self.session.auth = GraphAuth(
            self.client_id,
            self.scopes,
            data['access_token'],
            datetime.utcnow() + timedelta(seconds=data['expires_in']),
            data.get('refresh_token'),
            self.tenant,
        )

    def _get(self, url, *args, **kwargs):
        req = self.session.get(url, *args, **kwargs).json()
        if 'error' in req:
            raise APIError(req)
        return req

    def get_me_drive(self):
        return GraphDrive(self, self._get('https://graph.microsoft.com/v1.0/me/drive'))

    def get_site_drive(self, site_id: str):
        return GraphDrive(
            self, self._get('https://graph.microsoft.com/v1.0/sites/%s/drive' % site_id)
        )


class GraphDrive:
    def __init__(self, api: GraphAPI, data: dict) -> None:
        self._api = api
        self.data = data

    @property
    def id(self) -> str:
        return self.data['id']

    @property
    def root(self) -> 'GraphDriveRootItem':
        return GraphDriveRootItem(
            self,
            self._api._get('https://graph.microsoft.com/v1.0/drives/%s/root' % self.id),
        )

    def get_item_by_id(self, id: str) -> 'GraphDriveItem':
        return GraphDriveItem(
            self,
            self._api._get(
                'https://graph.microsoft.com/v1.0/drives/%s/items/%s' % (self.id, id)
            ),
        )

    def get_item_by_path(self, path: str) -> 'GraphDriveItem':
        if path == '/':
            return self.root
        path = path.rstrip('/')
        return GraphDriveItem(
            self,
            self._api._get(
                'https://graph.microsoft.com/v1.0/drives/%s/root:%s' % (self.id, path)
            ),
        )


class GraphDriveItem:
    def __init__(self, drive: GraphDrive, data: dict) -> None:
        self._drive = drive
        self._api = drive._api
        self.data = data

    @property
    def id(self) -> str:
        return self.data['id']

    @property
    def is_folder(self) -> bool:
        return 'folder' in self.data

    @property
    def is_file(self) -> bool:
        return 'file' in self.data

    @property
    def path(self) -> str:
        if 'path' in self.data['parentReference']:
            return self.data['parentReference']['path'] + '/' + self.data['name']
        return '/drives/%s/root' % self._drive.id

    @property
    def _baseurl(self):
        return 'https://graph.microsoft.com/v1.0%s:' % self.path

    @property
    def _pathurl(self):
        return 'https://graph.microsoft.com/v1.0%s' % self.path

    def list_children(
        self, nextlink: str | None = None, count: int = 200
    ) -> tuple[list['GraphDriveItem'], str | None]:
        url = nextlink or ('%s/children' % self._baseurl)
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)
        qs['$top'] = [str(count)]
        url = urllib.parse.urlunparse(parsed._replace(query=''))
        r = self._api._get(url, params=qs)
        return (
            [GraphDriveItem(self._drive, item) for item in r['value']],
            r.get('@odata.nextLink'),
        )

    def get_content_url(self) -> str:
        if not self.is_file:
            raise ValueError('Cannot get content of non-file')
        r = self._api.session.get('%s/content' % self._baseurl, allow_redirects=False)
        if (
            r.status_code > 399
            or r.status_code < 300
            or r.next is None
            or r.next.url is None
        ):
            raise APIError(r.text)
        return r.next.url

    def get_child_by_name(self, name: str):
        return GraphDriveItem(
            self._drive, self._api._get('%s/%s' % (self._pathurl, name))
        )

    def __truediv__(self, name: str):
        return self.get_child_by_name(name)


class GraphDriveRootItem(GraphDriveItem):
    @property
    def _baseurl(self):
        return 'https://graph.microsoft.com/v1.0/drives/%s/root' % self._drive.id

    @property
    def _pathurl(self):
        return 'https://graph.microsoft.com/v1.0/drives/%s/root:' % self._drive.id
