import dill
import copy
import asyncio
import uuid
from datetime import datetime


def db(host='127.0.0.1', port=17000):
    return DB(host, port)


class DB():
    def __init__(self, host='127.0.0.1', port=17000):
        self.host = host
        self.port = port
        self._async = False

    def shelf(self, shelf_name):
        return ShelfQuery(copy.copy(self), shelf_name)

    def asyncio(self):
        self._async = True
        return self

    def sync(self):
        self._async = False
        return self


class Item(dict):
    def __init__(self, id, data):
        self.id = id
        super().__init__(data)

    @property
    def timestamp(self):
        """Entry's timestamp from uuid1. Use formular from stack overflow.
        See in stackoverflow.com : https://bit.ly/2EtH05b
        """
        try:
            return self._timestamp
        except AttributeError:
            self._timestamp = datetime.fromtimestamp(
                (uuid.UUID(self.id).time - 0x01b21dd213814000)*100/1e9)
            return self._timestamp


class ShelfQuery():
    def __init__(self, db, shelf):
        self.db = db
        self.shelf = shelf
        self.queries = []
        self._make_run()

    def count(self):
        return ChainQuery(self, 'count')

    def delete(self):
        return ChainQuery(self, 'delete')

    def edit(self, func):
        return ChainQuery(self, {'edit': func})

    def get(self, id_):
        return ChainQuery(self, {'get': id_})

    def filter(self, filter_):
        return ChainQuery(self, {'filter': filter_})

    def first(self, filter_=None):
        return ChainQuery(self, {'first': filter_})

    def insert(self, entry):
        return ChainQuery(self, {'insert': entry})

    def map(self, map_):
        return ChainQuery(self, {'map': map_})

    def put(self, id_, entry):
        return ChainQuery(self, {'put': [id_, entry]})

    def reduce(self, reduce_):
        return ChainQuery(self, {'reduce': reduce_})

    def replace(self, entry):
        return ChainQuery(self, {'replace': entry})

    def slice(self, start, stop, step=None):
        return ChainQuery(self, {'slice': [start, stop, step]})

    def sort(self, key=None, reverse=False):
        return ChainQuery(self, {'sort': {'key': key, 'reverse': reverse}})

    def update(self, patch):
        return ChainQuery(self, {'update': patch})

    def _make_run(self):
        if self.db._async is True:
            self.run = self.run_async
        else:
            self.run = self.run_sync

    def run_sync(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(
            asyncio.ensure_future(self.run_async())
        )
        loop.close()
        return result

    async def run_async(self):
        queries = self.queries.copy()
        queries.insert(0, self.shelf)
        queries = dill.dumps(queries)
        reader, writer = await asyncio.open_connection(
            self.db.host,
            self.db.port,)
        writer.write(queries)
        writer.write_eof()
        await writer.drain()
        result = await reader.read(-1)
        result = dill.loads(result)
        writer.close()
        await writer.wait_closed()
        if isinstance(result, list):
            for i, item in enumerate(result):
                result[i] = Item(item[0], item[1])
        elif isinstance(result, tuple):
            result = Item(result[0], result[1])
        elif isinstance(result, Exception):
            raise result
        return result


class ChainQuery(ShelfQuery):
    def __init__(self, chain_query, query):
        self.db = chain_query.db
        self.shelf = chain_query.shelf
        self.queries = chain_query.queries.copy()
        self.queries.append(query)
        self._make_run()
