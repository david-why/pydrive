import json
import time
import traceback
import typing as t
from collections.abc import MutableMapping
from hashlib import md5
from pathlib import Path
from threading import RLock, Thread

from requests import Session

from .auth import GraphAuth
from .dbdict import DBDict
from .exceptions import APIError


def get_drive_cache_filename(drive_id: str) -> str:
    return 'drv-' + md5(drive_id.encode()).hexdigest()


def _json_dumps(*args, **kwargs):
    return json.dumps(*args, **kwargs).encode()


class _DrivesCache:
    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def __getitem__(self, id: str):
        # print('dbdict', self._path / md5(id.encode()).hexdigest())
        return DBDict(
            'cache',
            self._path / get_drive_cache_filename(id),
            dumps=_json_dumps,
            key_loads=json.loads,
            value_loads=json.loads,
        )


class _DriveCache(MutableMapping):
    def __init__(self, id: str, cache: MutableMapping) -> None:
        super().__init__()
        self._drive_id = id
        self._cache = cache

    def __getitem__(self, key: str):
        return self._cache['%s.%s' % (self._drive_id, key)]

    def __setitem__(self, key: str, value) -> None:
        self._cache['%s.%s' % (self._drive_id, key)] = value

    def __delitem__(self, key: str) -> None:
        del self._cache['%s.%s' % (self._drive_id, key)]

    def __iter__(self) -> t.Generator[str, None, None]:
        for key in self._cache:
            if isinstance(key, str) and key.startswith('%s.' % self._cache):
                yield key

    def __len__(self):
        count = 0
        for _ in self.__iter__():
            count += 1
        return count


class GraphDrives:
    def __init__(
        self,
        auth: GraphAuth,
        cache: t.MutableMapping | None = None,
        cachedir: str | None = None,
        poll_interval: float = 30,
    ) -> None:
        if cache is not None and cachedir is not None:
            raise ValueError('Cannot specify both cache and cachedir')
        self.session = Session()
        self.session.auth = auth
        self.cachedir = cachedir
        self.cache = {} if cache is None and cachedir is None else cache
        self._dcache = None if cachedir is None else _DrivesCache(cachedir)
        self.poll_interval = poll_interval

    def _get_drive_cache(self, id: str):
        if self.cache:
            return _DriveCache(id, self.cache)
        assert self._dcache
        return self._dcache[id]

    @property
    def auth(self) -> GraphAuth:
        return self.session.auth  # type: ignore

    @auth.setter
    def auth(self, auth: GraphAuth) -> None:
        self.session.auth = auth

    def get_drive(self, drive_id: str, **kwargs) -> 'GraphDrive':
        kwargs.setdefault('poll_interval', self.poll_interval)
        return GraphDrive(self, drive_id, **kwargs)

    def get_me_drive(self, **kwargs):
        r = self.session.get('https://graph.microsoft.com/v1.0/me/drive').json()
        return self.get_drive(r['id'], **kwargs)


def _get_path(item: dict):
    if 'root' in item:
        return '/'
    return item['parentReference']['path'].partition(':')[2] + '/' + item['name']


class GraphDrive:
    def __init__(
        self,
        drives: GraphDrives,
        id: str,
        poll_interval: float = 30,
        start_thread: bool = True,
    ) -> None:
        self._drives = drives
        # self._cache = _DriveCache(id, drives.cache)
        self._cache = drives._get_drive_cache(id)
        self._children_lock = RLock()
        self.id = id
        self.data = {}
        self.poll_interval = poll_interval
        self.refresh()
        self._delta_link = self._cache.get('delta', '')
        self._thread = Thread(target=self._poll_with_delta, daemon=True)
        if start_thread:
            self.start_thread()

    def start_thread(self):
        self._thread.start()

    @property
    def _delta_link(self) -> str:
        return self._cache.get('delta', '')

    @_delta_link.setter
    def _delta_link(self, link: str):
        self._cache['delta'] = link

    def _poll_with_delta(self):
        session = Session()
        session.auth = self._drives.auth
        while True:
            nextlink = self._delta_link or (
                'https://graph.microsoft.com/v1.0/drives/%s/root/delta' % self.id
            )
            while nextlink:
                # print('poll', nextlink)
                try:
                    r = session.get(nextlink).json()
                except:
                    traceback.print_exc()
                    time.sleep(self.poll_interval)
                    continue
                # print(r)
                items = r['value']
                nextlink = r.get('@odata.nextLink')
                if '@odata.deltaLink' in r:
                    self._delta_link = r['@odata.deltaLink']
                for item in items:
                    if 'deleted' in item:
                        self._deleted(item)
                        continue
                    path = _get_path(item)
                    if path not in self._cache:
                        self._added(item)
                    self._cache[path] = item
            time.sleep(self.poll_interval)

    def _deleted(self, item: dict):
        # print(item)
        path = _get_path(item)
        if path not in self._cache:
            return
        self._cache.pop(path)
        if 'folder' in item:
            self._cache.pop('c%s' % path)
        parent = path.rpartition('/')[0]
        paritem = self._cache.get(parent)
        assert paritem, 'Who is the parent of %s?' % path
        with self._children_lock:
            parchildren = self._cache.get('c%s' % parent, [])
            parchildren.remove(item['name'])
            self._cache['c%s' % parent] = parchildren

    def _added(self, item: dict):
        path = _get_path(item)
        parent = path.rpartition('/')[0] or '/'
        if 'root' not in item:
            with self._children_lock:
                parchildren = self._cache.get('c%s' % parent, [])
                parchildren.append(item['name'])
                self._cache['c%s' % parent] = parchildren

    def refresh(self) -> dict:
        req = self._drives.session.get(
            'https://graph.microsoft.com/v1.0/drives/%s' % self.id
        ).json()
        if 'error' in req:
            raise APIError(req)
        self.data = req
        return req

    @property
    def root(self) -> 'GraphDriveItem':
        return GraphDriveItem(self, '/')

    def get_item_by_path(self, path) -> 'GraphDriveItem | None':
        if path in self._cache:
            return GraphDriveItem(self, path)

    @property
    def name(self) -> str:
        return self.data['name']

    @property
    def quota(self) -> dict:
        return self.data['quota']

    @property
    def type(self) -> str:
        return self.data['driveType']


class GraphDriveItem:
    def __init__(self, drive: GraphDrive, path: str) -> None:
        self._drive = drive
        self._session = self._drive._drives.session
        self.path = path

    @property
    def data(self) -> dict:
        return self._drive._cache[self.path]

    @property
    def children(self) -> list[str] | None:
        if 'folder' not in self.data:
            return None
        with self._drive._children_lock:
            return self._drive._cache.get('c%s' % self.path, [])

    @property
    def _pathurl(self):
        if self.path == '/':
            return 'https://graph.microsoft.com/v1.0/drives/%s/root:' % self._drive.id
        return 'https://graph.microsoft.com/v1.0/drives/%s/root:%s' % (
            self._drive.id,
            self.path,
        )

    @property
    def _propurl(self):
        if self.path == '/':
            return 'https://graph.microsoft.com/v1.0/drives/%s/root' % self._drive.id
        return 'https://graph.microsoft.com/v1.0/drives/%s/root:%s:' % (
            self._drive.id,
            self.path,
        )

    @property
    def is_file(self) -> bool:
        return 'file' in self.data

    @property
    def is_folder(self) -> bool:
        return 'folder' in self.data

    @property
    def size(self) -> int:
        return self.data['size']

    def get_contents(
        self, offset: int | None = None, size: int | None = None
    ) -> bytes | None:
        if self.is_folder:
            return None
        if size is not None and size <= 0:
            return b''
        ctag = self._drive._cache.get('t%s' % self.path)
        headers = {} if ctag is None else {'if-none-match': ctag}
        if offset is not None or size is not None:
            offset = offset or 0
            headers['range'] = 'bytes=%d-%s' % (
                offset,
                (offset + size - 1) if size else '',
            )
        r = self._session.get(
            '%s/content' % self._propurl, headers=headers, allow_redirects=False
        )
        # print(headers)
        # print(r, r.headers)
        if r.status_code == 304:
            return b''  # TODO
        if r.status_code >= 400:
            return
        if r.next:
            return self._session.send(r.next).content
        return r.content
