import logging
import time
from collections import deque

from thespian.actors import Actor

from thor.actors import DirectoryServer, Server
from thor.utils import Aggregator, Transaction


class NotCached(Exception):
    pass


class Clerk(Actor):
    class View:
        def __init__(self, directory_server, key_space):
            self.directory_server = directory_server
            self.key_space = key_space

    class Read:
        def __init__(self, oids):
            self.oids = oids

    class Commit:
        def __init__(self, trx):
            self.trx = trx

    class InvalidateCache:
        pass

    def __init__(self, globalName=None):
        self.directory_server = None
        self.object_cache = {}
        self.server_cache = {}
        self.trx_counter = 1
        self.key_space = None

        self.pending_reads = deque()
        self.trx_reading = {}
        self.trx_preparing = {}
        self.trx_prepared = {}
        super().__init__(globalName=globalName)

    def receiveMessage(self, msg, sender):
        if isinstance(msg, Clerk.View):
            logging.debug("View %s", self.globalName)
            self.directory_server = msg.directory_server
            self.send(msg.directory_server, DirectoryServer.RegisterListener())
            self.key_space = msg.key_space
            self.send(sender, True)

        elif isinstance(msg, Clerk.Read):
            self.pending_reads.append((sender, msg.oids))
            self.check_pending_reads()
        elif isinstance(msg, Clerk.Commit):
            self.start_prepare(sender, msg)

        elif isinstance(msg, Aggregator.Gather):
            if msg.trx.status == "R":
                self.read_done(msg)
            elif msg.trx.status == "E":
                self.prepare_done(msg)
            elif msg.trx.status == "P":
                self.commit_done(msg)

        elif isinstance(msg, DirectoryServer.WhoServes):
            self.server_cache.update(msg.server_map)
            logging.debug("Server map of %s updated", self.globalName)
            self.check_pending_reads()

        elif isinstance(msg, Clerk.InvalidateCache):
            pass

    def _get_who_serves(self, oids):
        if set(oids) - self.server_cache.keys():
            raise NotCached("clerk has not received mapping for all oids")

        return self._group_by_server({
            oid: server
            for oid, server in self.server_cache.items() if oid in oids
        })

    def check_pending_reads(self):
        failed = deque()
        while self.pending_reads:
            sender, oids = self.pending_reads.pop()
            try:
                self.start_read(sender, oids)
            except NotCached:
                failed.append((sender, oids))

        self.pending_reads = failed

    def _group_by_server(self, server_map: dict) -> dict:
        grouped_server_map = {}
        for oid, server in server_map.items():
            if server not in grouped_server_map:
                grouped_server_map[server] = set()
            grouped_server_map[server].add(oid)
        return grouped_server_map

    def start_read(self, sender, oids):
        read_set = {k: None for k in oids}
        server_map = self._get_who_serves(read_set.keys())
        logging.debug("%s -Sending reads to %s ", self.globalName, server_map.keys())

        trx = Transaction("{}-trx-{}".format(self.globalName, self.trx_counter), read_set, {}, None)
        self.trx_counter += 1
        aggregator = self.createActor(Aggregator, globalName="aggregator-%s-r" % trx.tid)
        self.send(aggregator, Aggregator.Scatter(trx, server_map, Server.Objects))
        self.trx_reading[trx.tid] = (sender, trx, aggregator)

    def read_done(self, gather):
        trx = gather.trx
        client = self.trx_reading[trx.tid][0]
        logging.debug("%s -Read finished for %s", self.globalName, trx.tid)

        self.trx_reading.pop(trx.tid)
        for ans in gather.answers:
            self.object_cache.update(ans[1])
            trx.read_set.update(ans[1])
        trx.write_set = {k: v[0] for k, v in trx.read_set.items()}
        trx.status = "E"
        self.send(client, trx)

    def start_prepare(self, sender, msg):
        trx = msg.trx
        trx.timestamp = round(time.time() * 1000)
        server_map = self._get_who_serves(trx.read_set.keys())
        #logging.debug("%s -Committing %s", self.globalName, trx.tid)
        logging.debug("%s -Sending prepare to %s ", self.globalName, server_map.keys())

        aggregator = self.createActor(Aggregator, globalName="aggregator-%s-p" % trx.tid)
        self.send(aggregator, Aggregator.Scatter(trx, server_map, Server.Prepare))
        self.trx_preparing[trx.tid] = (sender, trx, aggregator)

    def prepare_done(self, gather):
        trx = gather.trx
        client = self.trx_preparing[trx.tid][0]
        servers = (v[0] for v in gather.answers)
        all_good = all(v[1] for v in gather.answers)

        if not all_good:
            self.trx_preparing.pop(trx.tid)
            logging.debug("%s -Aborting due to validation fail %s", self.globalName, trx.tid)
            for server in servers:
                if gather.answers[server]:
                    self.send(server, Server.Abort(trx))
            self.send(client, False)
        else:
            self.trx_preparing.pop(trx.tid)
            trx.status = "P"
            server_map = self._get_who_serves(trx.read_set.keys())
            logging.debug("%s -Received all prepare ok and sending commit to %s ", self.globalName, server_map.keys())

            aggregator = self.createActor(Aggregator, globalName="aggregator-%s-c" % trx.tid)
            self.send(aggregator, Aggregator.Scatter(trx, server_map, Server.Commit))
            self.trx_prepared[trx.tid] = (client, trx, aggregator)

    def commit_done(self, gather):
        trx = gather.trx
        client = self.trx_prepared[trx.tid][0]
        logging.debug("%s -Committed %s ", self.globalName, trx.tid)

        self.trx_prepared.pop(trx.tid)
        self.send(client, True)
