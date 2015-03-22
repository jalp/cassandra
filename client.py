import logging
from cassandra.cluster import Cluster, NoHostAvailable, RetryPolicy
from uuid import uuid1

log = logging.getLogger("cassandra")
log.setLevel("INFO")
file_handler = logging.FileHandler("cassandra.log")
stream_handler = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

log.addHandler(stream_handler)
log.addHandler(file_handler)

_session = None


class Connection:
    def get_connection(self, name):
        global _session
        if not _session:
            try:
                cluster = Cluster(contact_points=["10.0.0.2"], default_retry_policy=RetryPolicy())
                _session = cluster.connect(name)
                log.info("Session established with '{}'".format(cluster.metadata.cluster_name))
            except NoHostAvailable as nohostex:
                raise SystemError("No hosts availables. Reasons: {}".format(nohostex.errors))
        return _session

    def close(self):
        _session.cluster.shutdown()
        _session.shutdown()
        log.info("Connection closed")


class Dao:
    def __init__(self, name):
        client = Connection()
        self.name = name
        self.session = client.get_connection(name)


class RequestDao(Dao):
    def __init__(self, name):
        super().__init__(name)

    def create_schema(self):
        replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        log.info("CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {};".format(self.name, replication))
        self.session.execute(
            """CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {};""".format(self.name, replication))
        log.info("Keyspace created")
        self.session.execute(
            "CREATE TABLE IF NOT EXISTS ads (id uuid, description text, price varint, PRIMARY KEY(id))")
        log.info("Ads table created")

    def remove_schema(self):
        self.session.execute("DROP KEYSPACE {}".format(self.name))
        log.info("Keyspace '{}' dropped".format(self.name))

    def select(self, table):
        log.info("selecting data from table {}".format(table))
        return self.session.execute("SELECT * FROM {}".format(table))

    def insert(self, stmt):
        log.info("Insert statement: {}".format(stmt))
        return self.session.execute(stmt)


def main():
    conn = Connection()
    rd = RequestDao("demo")

    rd.remove_schema()

    rd.create_schema()

    rd.insert("INSERT INTO ads (id, description, price) VALUES ({}, '{}', {})".format(uuid1(), "blablablabla", 10))

    rows = rd.select("ads")
    for row in rows:
        log.info(row.id)
    conn.close()


if __name__ == "__main__":
    main()