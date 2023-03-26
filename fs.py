import errno
import json
import os
import stat
import sys
from datetime import datetime
from pathlib import Path

import fuse

from .auth import GraphAuth
from .dbdict import DBDict
from .fsapi import GraphDrives

fuse.fuse_python_api = (0, 2)

# CACHE = Path(__file__).parent.joinpath('cfg')
CACHE = Path('~/.config/pydrive').expanduser()
CACHE.mkdir(parents=True, exist_ok=True)

CLIENT_ID = 'bcc98b3d-df9e-43ac-929c-08b5b7b07648'
SCOPES = ['User.Read', 'Sites.ReadWrite.All', 'offline_access']


def _path2slug(path):
    return 'mnt' + os.fspath(path).replace('/', '-')


def _json_dumps(*args, **kwargs):
    return json.dumps(*args, **kwargs).encode()


class Stat:
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class OnedriveFS(fuse.Fuse):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.parser.add_option(
            '-D',
            '--drive',
            dest='drive',
            help='drive ID to mount, default user\'s OneDrive',
            default=None,
        )

    def main(self):
        options = self.cmdline[0]
        self.cachefn = CACHE / _path2slug(self.fuse_args.mountpoint)
        self.cache = DBDict(
            'cache',
            self.cachefn,
            dumps=_json_dumps,
            key_loads=json.loads,
            value_loads=json.loads,
        )
        refresh = self.cache.get('refresh_token')
        if not refresh:
            self.graph = GraphAuth(CLIENT_ID, SCOPES)
            self.graph.authenticate()
            self.cache['refresh_token'] = self.graph.refresh_token
            # with self.cachefn.open('w') as f:
            #     json.dump(cache, f)
        else:
            self.graph = GraphAuth(CLIENT_ID, SCOPES, refresh_token=refresh)
        # self.drives = GraphDrives(self.graph, cache=self.cache)
        self.drives = GraphDrives(self.graph, cachedir=str(CACHE))
        drive_id = options.drive
        if drive_id is None:
            self.drive = self.drives.get_me_drive(start_thread=False)
        else:
            self.drive = self.drives.get_drive(drive_id, start_thread=False)
        self.root = self.drive.root
        return super().main(self.fuse_args.assemble())

    def fsinit(self):
        self.drive.start_thread()

    def getattr(self, path):
        file = self.drive.get_item_by_path(path)
        if file is None:
            return -errno.ENOENT
        st = Stat()
        if file.is_folder:
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_size = 4096
        else:
            st.st_mode = stat.S_IFREG | 0o644
            st.st_size = file.data['size']
            # raise ValueError('Neither regular file nor folder: %s' % path)
        st.st_uid = os.getuid()
        st.st_gid = os.getgid()
        st.st_atime = st.st_mtime = st.st_ctime = int(
            datetime.fromisoformat(
                file.data['lastModifiedDateTime'].rstrip('Z') + '+00:00'
            ).timestamp()
        )
        return st

    def open(self, path, flags):
        file = self.drive.get_item_by_path(path)
        if file is None:
            return -errno.ENOENT
        if not file.is_file:
            return -errno.ENOENT
        if flags & (os.O_RDWR | os.O_WRONLY):
            return -errno.ENOTSUP

    def readdir(self, dir, offset):
        if offset != 0:
            return -errno.ENOTSUP
        folder = self.drive.get_item_by_path(dir)
        if folder is None or not folder.is_folder:
            return -errno.ENOENT
        entries = [
            fuse.Direntry('.', type=stat.S_IFDIR),
            fuse.Direntry('..', type=stat.S_IFDIR),
        ]
        for item in folder.children:  # type: ignore
            path = '%s/%s' % (dir.rstrip('/'), item)
            itemobj = self.drive.get_item_by_path(path)
            if itemobj is None:
                continue
            if itemobj.is_folder:
                typ = stat.S_IFDIR
            else:
                typ = stat.S_IFREG
            entries.append(fuse.Direntry(item, type=typ))
        return entries

    def read(self, path, size, offset):
        item = self.drive.get_item_by_path(path)
        if item is None:
            return -errno.ENOENT
        contents = item.get_contents(offset, size)
        if contents is None:
            return contents
        return contents


def main():
    drv = OnedriveFS()
    drv.parse(errex=1)
    # options = drv.cmdline[0]
    if drv.fuse_args.mount_expected():
        if drv.fuse_args.mountpoint is None:
            drv.parser.print_help()
            print('error: no mountpoint', file=sys.stderr)
            return
        drv.main()


if __name__ == '__main__':
    main()
