from threading import RLock
import time
import typing as t
from datetime import datetime, timedelta

from requests import Session
from requests.auth import AuthBase
from requests.models import PreparedRequest

from .exceptions import (
    APIError,
    AuthorizationCanceledError,
    AuthorizationTimeoutError,
    NoTokenError,
)


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
        self.client_id = client_id
        self.scopes = scopes
        self._access_token = access_token
        self.expires = expires or datetime.utcnow()
        self.refresh_token = refresh_token
        self.tenant = tenant
        self.session = session or Session()
        self._lock = RLock()

    @property
    def access_token(self) -> str:
        if self._access_token is None and self.refresh_token is None:
            raise NoTokenError('No refresh token or access token')
        if datetime.utcnow() > self.expires or self._access_token is None:
            self.refresh()
        return self._access_token  # type: ignore

    def refresh(self) -> str:
        if self.refresh_token is None:
            raise NoTokenError('No refresh token')
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
        with self._lock:
            self.expires = datetime.utcnow() + timedelta(seconds=r['expires_in'])
            self.refresh_token = r['refresh_token']
            self._access_token = r['access_token']
        return self._access_token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        if 'Authorization' not in r.headers:
            r.headers['Authorization'] = 'Bearer ' + self.access_token
        return r

    def authenticate(
        self, callback: t.Callable[[str, str, datetime], None] | None = None
    ):
        r = self.session.post(
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
            r = self.session.post(
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
            raise AuthorizationTimeoutError('User authorization timed out')
        with self._lock:
            self.expires = datetime.utcnow() + timedelta(seconds=data['expires_in'])
            self._access_token = data['access_token']
            self.refresh_token = data.get('refresh_token', self.refresh_token)
        return self._access_token
