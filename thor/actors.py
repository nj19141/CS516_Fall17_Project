import logging
import time

from sortedcontainers import SortedListWithKey
from thespian.actors import Actor

from .utils import Transaction


class Server(Actor):
    THRESHOLD = 120

    class View:
        def __init__(self, keys: set):
            self.keys = keys

    class Objects:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class Prepare:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class Commit:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class Abort:
        def __init__(self, transaction: Transaction):
            self.transaction = transaction

    class CacheInvalidate:
        def __init__(self, oid):
            self.oid = oid

    def __init__(self, globalName=None):
        self.store = {}
        self.cache_table = {}
        self.history = SortedListWithKey(key=lambda t: t.timestamp)
        super().__init__(globalName=globalName)

    def receiveMessage(self, msg, sender):
        if isinstance(msg, Server.Objects):
            # logging.debug("%s received read object request for %s", self.globalName, msg.transaction.read_set.keys())
            self.send(sender, self.get_objects(msg))
        elif isinstance(msg, Server.Prepare):
            self.send(sender, self.prepare_trx(msg))
        elif isinstance(msg, Server.Commit):
            self.send(sender, self.commit_trx(msg))
        elif isinstance(msg, Server.Abort):
            self.send(sender, self.abort_trx(msg))
        elif isinstance(msg, Server.View):
            logging.debug("Server View  of %s", self.globalName)
            self.store = {k: (0, 0, 0) for k in msg.keys}
            self.send(sender, True)

    def get_objects(self, msg):
        answer = {}
        for oid in msg.transaction.read_set.keys():
            answer[oid] = self.store[oid][:-1]
            if oid not in self.cache_table:
                self.cache_table[oid] = []
            self.cache_table[oid].append(msg.transaction.clerk)
        return answer

    def truncate_history(self):
        threshold = round(time.time() * 1000) - Server.THRESHOLD
        to_delete = self.history[:self.history.bisect_key_left(threshold)]
        for item in to_delete:
            self.history.remove(item)

    def prepare_trx(self, msg: Prepare) -> bool:
        # self.truncate_history()
        trx = msg.transaction
        position = self.history.bisect_key_left(trx.timestamp)
        earlier_trxs, later_trxs = self.history[:position], \
                                   self.history[position:]

        # logging.warning("validating %s, position in timeline: %d, earlier %s, later %s", trx.tid, position, earlier_trxs, later_trxs)

        # validate against earlier transactions
        for earlier_t in earlier_trxs:
            read_set_oids = set(trx.read_set.keys())
            if earlier_t.write_set.keys() & read_set_oids:
                logging.debug("%s -Validation failed for %s - Against earlier transactions - Prepare NO", self.globalName, trx.tid)
                return False


        # validate against r_stamp
        for oid, shadow in trx.read_set.items():
            store_version = self.store[oid][1]
            r_stamp = self.store[oid][2]
            # if shadow[1] != store_version:
            if trx.timestamp < r_stamp or shadow[1] != store_version:
                logging.debug("%s -conflict, %s, %s", self.globalName, shadow, self.store[oid])
                # logging.debug("%s -Validation failed for %s - transaction timestamp < rstamp - Prepare NO", self.globalName, trx.tid)
                return False

        # validate against later transactions
        for later_t in later_trxs:
            later_t_read_set_oids = set(later_t.read_set.keys())
            trx_read_set_oids = set(trx.read_set.keys())

            if trx.write_set.keys() & later_t_read_set_oids or \
               trx_read_set_oids & later_t.write_set.keys():
                logging.debug("%s -Validation failed for %s - Against later transactions - Prepare NO", self.globalName, trx.tid)
                return False

        trx.status = "P"
        self.history.add(trx)
        logging.debug("%s Validation successful for %s - Prepare OK", self.globalName, trx.tid)
        return True

    def commit_trx(self, msg: Commit) -> bool:
        trx = msg.transaction
        logging.debug("%s -Committing %s with \nread_set: %s\nwrite_set: %s", self.globalName, trx.tid, trx.read_set.keys(), trx.write_set.keys())
        try:
            index = self.history.index(trx)
            if self.history[index].status != "P":
                logging.debug("Transaction failed to commit")
                return False
        except ValueError:
            return False

        for oid, _ in trx.read_set.items():
            val = self.store[oid]
            self.store[oid] = (val[0], val[1], trx.timestamp)

        for oid, val in trx.write_set.items():
            store_version = self.store[oid][1]
            self.store[oid] = (val, store_version + 1, trx.timestamp)
            for clerk in self.cache_table.get(oid, []):
                logging.debug("%s -Sending cache invalidation for %d", self.globalName, oid)
                self.send(clerk, Server.CacheInvalidate(oid))
            self.cache_table[oid].clear()

        trx.status = "C"
        self.history.remove(trx)
        return True

    def abort_trx(self, msg: Abort) -> bool:
        logging.debug("%s - Aborting transaction %s", self.globalName, msg.transaction.tid)
        logging.debug("%s - history is: %s", self.globalName, self.history)
        self.history.remove(msg.transaction)
        return True


class DirectoryServer(Actor):
    class RegisterListener:
        pass

    class GetTimestamp:
        pass

    class WhoServes:
        def __init__(self, server_map: dict):
            self.server_map = server_map

    def __init__(self, globalName=None):
        self.server_map = {}
        self.listeners = []
        super().__init__(globalName=globalName)

    def receiveMessage(self, msg, sender):
        if isinstance(msg, DirectoryServer.RegisterListener):
            #logging.debug("Register Listener for %s", self.globalName)
            self.listeners.append(sender)
            self.send(sender, DirectoryServer.WhoServes(self.server_map))
        elif isinstance(msg, DirectoryServer.WhoServes):
            logging.debug("Who serves %s", self.globalName)
            self.server_map.update(msg.server_map)
            for listener in self.listeners:
                self.send(listener, DirectoryServer.WhoServes(self.server_map))
            self.send(sender, True)
        elif isinstance(msg, DirectoryServer.GetTimestamp):
            logging.debug("Getting simeStamp for %s", self.globalName)
            self.send(sender, round(time.time() * 1000))
