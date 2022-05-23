import json
import logging
import os
from datetime import datetime

from dataset import connect

from configurable_http_proxy.store import BaseStore

log = logging.getLogger(__name__)


class DatabaseStore(BaseStore):
    """A DBMS storage backend for configurable-http-proxy

    This enables chp to run multiple times and serve routes from a central
    DBMS. It uses SQLAlchemy as the database backend.

    Usage:
        Set the CHP_DATABASE_URL env var to any db URL supported by SQLAlchemy.
        The default is "sqlite://chp.sqlite".

        $ export CHP_DATABASE_URL="sqlite:///chp.sqlite"
        $ configurable-http-proxy --storage-backend configurable_http_proxy.dbstore.DatabaseStore

        Optionally you may set the table name by setting the CHP_DATABASE_TABLE.
        The default is 'chp_routes'

        $ export CHP_DATABASE_TABLE="chp_routes"

    See Also:
        * Valid URLs https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
    """

    default_db_url = "sqlite:///chp.sqlite"
    default_db_table = "chp_routes"

    def __init__(self):
        super().__init__()
        db_url = os.environ.get("CHP_DATABASE_URL", self.default_db_url)
        db_table = os.environ.get("CHP_DATABASE_TABLE", self.default_db_table)
        self.routes: TableTrie = TableTrie(db_url, table=db_table)
        log.info(f"Using database {db_url}")
        for route, data in self.get_all().items():
            log.info(f'Restoring {route} => {data.get("target", "<no target>")}')

    def clean(self):
        # remove all information stored so far
        self.routes.clean()

    def get_target(self, path: str):
        # return the data for the most specific matching route
        return self.routes.get(self.clean_path(path), trie=True)

    def get_all(self):
        # return all routes as route => data
        return self.routes.all()

    def add(self, path: str, data):
        # add a new route /path, storing data
        if self.get(path):
            self.update(path, data)
        else:
            self.routes.add(path, data)

    def update(self, path: str, data):
        # update an existing route
        self.routes.update(self.clean_path(path), data)

    def remove(self, path: str):
        # remove an existing route
        path = self.clean_path(path)
        route = self.routes.get(path)
        if route:
            self.routes.remove(path)
        return route

    def get(self, path):
        # return the data for the exact match
        return self.routes.get(self.clean_path(path))


class TableTrie:
    # A databased URLTrie-alike
    def __init__(self, url, table=None):
        table = table or "chp_routes"
        self.db = connect(url)
        self.table = self.db[table]

    def get(self, path, trie=False):
        # return the data store for path
        # -- if trie is False (default), will return data for the exact path
        # -- if trie is True, will return the data and the matching prefix
        try_routes = self._split_routes(path) if trie else [path]
        for path in try_routes:
            doc = self.table.find_one(path=path, order_by="id")
            if doc:
                if not trie:
                    data = self._from_json(doc["data"])
                else:
                    data = doc
                    data["data"] = self._from_json(doc["data"])
                    data["prefix"] = path
                break
        else:
            data = None
        return attrdict(data) if data else None

    def add(self, path, data):
        # add the data for the given exact path
        self.table.insert({"path": path, "data": self._to_json(data)})

    def update(self, path, data):
        # update the data for the given exact path
        doc = self.table.find_one(path=path, order_by="id")
        doc["data"] = self._from_json(doc["data"])
        doc["data"].update(data)
        doc["data"] = self._to_json(doc["data"])
        self.table.update(doc, "id")

    def remove(self, path):
        # remove all matching routes for the given path
        for path in self._split_routes(path):
            self.table.delete(path=path)

    def all(self):
        # return all data for all paths
        return {item["path"]: self._from_json(item["data"]) for item in self.table.find(order_by="id")}

    def _to_json(self, data):
        # simple converter for serializable data
        for k, v in dict(data).items():
            if isinstance(v, datetime):
                data[k] = f"_dt_:{v.isoformat()}"
            elif isinstance(v, dict):
                data[k] = self._to_json(v)
        return json.dumps(data)

    def _from_json(self, data):
        # simple converter from serialized data
        data = json.loads(data) if isinstance(data, (str, bytes)) else data
        for k, v in dict(data).items():
            if isinstance(v, str) and v.startswith("_dt_:"):
                data[k] = datetime.fromisoformat(v.split(":", 1)[-1])
            elif isinstance(v, dict):
                data[k] = self._from_json(v)
        return data

    def _split_routes(self, path):
        # generator for reverse tree of routes
        # e.g. /path/to/document
        # => yields /path/to/document, /path/to, /path, /
        levels = path.split("/")
        for i, e in enumerate(levels):
            yield "/".join(levels[: len(levels) - i + 1])
        # always yield top level route
        yield "/"

    def clean(self):
        self.table.delete()


class attrdict(dict):
    # enable .attribute for dicts
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self
