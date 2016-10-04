import redis

class RedisCupboard:
    def __init__(self, cleanup):
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)
        if cleanup:
            self.redis.flushdb()

    def put_on_shelf(self, table, *keys):
        records_heap = 'table:' + table
        tables_heap = 'tables'
        self.redis.sadd(tables_heap, table)
        self.redis.sadd(records_heap, *keys)

    def put_on_reference_shelf(self, references):
        reftables_heap = 'reftables'
        for reference in references:
            table, primary_key = reference
            references_heap = 'ref:' + table
            records_heap = 'table:' + table
            if not self.redis.sismember(records_heap, primary_key):
                self.redis.sadd(reftables_heap, table)
                self.redis.sadd(references_heap, primary_key)

    def has_pending_references(self):
        reftables_heap = 'reftables'
        return self.redis.scard(reftables_heap) > 0

    def get_all_references(self):
        reftables_heap = 'reftables'
        tables = self.redis.smembers(reftables_heap)
        for table in tables:
            references_heap = 'ref:' + table.decode('utf-8')
            records_heap = 'table:' + table.decode('utf-8')
            references = self.redis.sdiff(references_heap, records_heap)
            print('     ', references_heap, records_heap, len(references))
            self.redis.delete(references_heap)
            self.redis.srem(reftables_heap, table.decode('utf-8'))
            yield (table, references,)

    def clear_shelf(self, table):
        records_heap = 'table:' + table
        self.redis.delete(records_heap)
