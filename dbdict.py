import pickle
import sqlite3
import threading
import typing as t

__all__ = ['DBDict']

ConvertType = t.Union[t.Type[int], t.Type[str], t.Type[bytes], t.Type[float]]
KT = t.TypeVar('KT')
VT = t.TypeVar('VT')


class CursorLike(t.Protocol):
    def execute(self, __sql: str, __params: t.Sequence = ...) -> t.Any:
        ...

    def fetchall(self) -> t.Iterable[t.Sequence]:
        ...

    def fetchone(self) -> t.Sequence:
        ...

    @property
    def rowcount(self) -> int:
        ...


class ConnectionLike(t.Protocol):
    def cursor(self) -> CursorLike:
        ...

    def commit(self) -> t.Any:
        ...


class DBAPILike(t.Protocol):
    connect: t.Callable[..., ConnectionLike]


class DBDict(t.MutableMapping[KT, VT], t.Generic[KT, VT]):
    def __init__(
        self,
        table: str,
        *dbargs,
        dbapi: DBAPILike = sqlite3,
        dumps: t.Callable[[t.Union[KT, VT]], bytes] = pickle.dumps,
        key_loads: t.Callable[[bytes], KT] = pickle.loads,
        value_loads: t.Callable[[bytes], VT] = pickle.loads,
    ) -> None:
        self._dbapi = dbapi
        self._args = dbargs
        self._orig_table = table
        table = self._quote(table)
        self._table = table
        self._dumps = dumps
        self._key_loads = key_loads
        self._value_loads = value_loads
        self._connections_lock = threading.Lock()
        self._connections: t.Dict[int, ConnectionLike] = {}
        con = self.connection
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS %s(key BLOB UNIQUE PRIMARY KEY, value BLOB);'
            % table
        )
        con.commit()

    @classmethod
    def _new(
        cls, table, dbargs, dbapi=None, dumps=None, key_loads=None, value_loads=None
    ):
        return cls(
            table,
            *dbargs,
            dbapi=dbapi or sqlite3,
            dumps=dumps or pickle.dumps,
            key_loads=key_loads or pickle.loads,
            value_loads=value_loads or pickle.loads,
        )

    def __reduce__(self):
        return type(self)._new, (
            self._orig_table,
            self._args,
            self._dbapi if self._dbapi is not sqlite3 else None,
            self._dumps if self._dumps is not pickle.dumps else None,
            self._key_loads if self._key_loads is not pickle.loads else None,
            self._value_loads if self._value_loads is not pickle.loads else None,
        )

    @staticmethod
    def _quote(string: str) -> str:
        return "'" + string.replace("'", "''") + "'"

    @property
    def connection(self) -> ConnectionLike:
        ident = threading.get_ident()
        if ident in self._connections:
            return self._connections[ident]
        with self._connections_lock:
            return self._connections.setdefault(ident, self._dbapi.connect(*self._args))

    @property
    def _cursor(self) -> CursorLike:
        return self.connection.cursor()

    def __getitem__(self, key: KT) -> VT:
        pickled = self._dumps(key)
        cur = self._cursor
        cur.execute('SELECT value FROM %s WHERE key=?' % self._table, (pickled,))
        row = cur.fetchone()
        if row is None:
            raise KeyError(key)
        return self._value_loads(row[0])

    def __setitem__(self, key: KT, value: VT) -> None:
        con = self.connection
        cur = con.cursor()
        key_pickled = self._dumps(key)
        value_pickled = self._dumps(value)
        if key in self:
            cur.execute(
                'UPDATE %s SET value=? WHERE key=?' % self._table,
                (value_pickled, key_pickled),
            )
        else:
            cur.execute(
                'INSERT INTO %s(key, value) VALUES(?, ?)' % self._table,
                (key_pickled, value_pickled),
            )
        con.commit()

    def __delitem__(self, key: KT) -> None:
        if key not in self:
            raise KeyError(key)
        con = self.connection
        cur = con.cursor()
        pickled = self._dumps(key)
        cur.execute('DELETE FROM %s WHERE key=?' % self._table, (pickled,))
        con.commit()

    def __iter__(self) -> t.Generator[KT, None, None]:
        cur = self._cursor
        cur.execute('SELECT key FROM %s' % self._table)
        rows = cur.fetchall()
        for (x,) in rows:
            yield self._key_loads(x)

    def __len__(self) -> int:
        cur = self._cursor
        cur.execute('SELECT key FROM %s' % self._table)
        return max(cur.rowcount, 0)

    def __eq__(self, d) -> bool:
        if not isinstance(d, t.Mapping):
            return NotImplemented
        if len(d) != len(self):
            return False
        _none = object()
        for k in self:
            v = d.get(k, _none)
            if v is _none or v != self[k]:
                return False
        return True

    def __repr__(self) -> str:
        return '<DBDict table="%s" at 0x%x>' % (self._orig_table, id(self))


if __name__ == '__main__':
    d = DBDict('data', 'data.db')
    print(d)
