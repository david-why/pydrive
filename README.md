# pydrive
Mount a OneDrive or SharePoint drive with [FUSE](https://github.com/libfuse/libfuse).

## Usage
```shell
$ python3 -m pydrive
Usage: __main__.py [mountpoint] [options]

Options:
    -h, --help             show this help message and exit
    -o opt,[opt...]        mount options
    -D DRIVE, --drive=DRIVE
                           drive ID to mount, default user's OneDrive
```

Additional options passed to FUSE include `-f` to run in foreground and `-d` for debug output.
