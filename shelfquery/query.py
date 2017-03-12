import dill, copy, asyncio


def connect(host='127.0.0.1', port=17000):
    return DB(host, port)

class DB():
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def shelf(self, shelf_name):
        return ShelfQuery(copy.copy(self), shelf_name)


class ShelfQuery():
    def __init__(self, db, shelf):
        self.db = db
        self.shelf = shelf
        self.queries = []

    def get(self, id_):
        return ChainQuery(self, {'get': id_})

    def first(self, filter_):
        return ChainQuery(self, {'first': filter_})

    def entry(self, fn):
        return ChainQuery(self, {'entry': fn})

    def filter(self, filter_):
        return ChainQuery(self, {'filter': filter_})

    def map(self, map_):
        return ChainQuery(self, {'map': map_})

    def reduce(self, reduce_):
        return ChainQuery(self, {'reduce': reduce_})

    def slice(self, start, stop, step=None):
        return ChainQuery(self, {'slice': [start, stop, step]})

    def sort(self, key=None, reverse=False):
        return ChainQuery(self, {'sort': {'key': key, 'reverse': reverse}})

    def update(self, patch):
        return ChainQuery(self, {'update': patch})

    def insert(self, entry):
        return ChainQuery(self, {'insert': entry})

    def replace(self, entry):
        return ChainQuery(self, {'replace': entry})

    def delete(self):
        return ChainQuery(self, 'delete')

    def run(self):
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
        result = await reader.read(-1)
        result = dill.loads(result)
        writer.close()
        return result

class ChainQuery(ShelfQuery):
    def __init__(self, chain_query, query):
        self.db = chain_query.db
        self.shelf = chain_query.shelf
        self.queries = chain_query.queries.copy()
        self.queries.append(query)
