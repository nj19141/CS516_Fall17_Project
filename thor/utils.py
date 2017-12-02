from thespian.actors import Actor, ActorExitRequest


class Aggregator(Actor):
    class Scatter:
        def __init__(self, trx, server_map, msg_class):
            self.trx = trx
            self.server_map = server_map
            self.msg_class = msg_class

    class Gather:
        def __init__(self, trx, answers):
            self.trx = trx
            self.answers = answers

    def __init__(self, globalName=None):
        self.trx = None
        self.asker = None
        self.asked = []
        self.answers = []
        super().__init__(globalName=globalName)

    def receiveMessage(self, msg, sender):
        if isinstance(msg, Aggregator.Scatter):
            #logging.debug("%s scattering %s for %s", self.globalName, msg.msg_class.__name__, msg.trx.tid)
            self.trx = msg.trx
            self.asker = sender

            for server_name, oids in msg.server_map.items():
                server = self.createActor(None, globalName=server_name)
                trx = Transaction(
                    msg.trx.tid,
                    {k: v
                     for k, v in msg.trx.read_set.items() if k in oids},
                    {k: v
                     for k, v in msg.trx.write_set.items()
                     if k in oids}, msg.trx.timestamp)
                self.asked.append(server)
                self.send(server, msg.msg_class(trx))

        else:
            if sender not in self.asked:
                return
            self.answers.append((sender, msg))
            if len(self.asked) == len(self.answers):
                #logging.debug("%s gathered", self.globalName)
                self.send(self.myAddress, ActorExitRequest())
                self.send(self.asker, Aggregator.Gather(self.trx, self.answers))


class Transaction:
    def __init__(self, tid, read_set, write_set, timestamp):
        self.tid = tid
        self.read_set = read_set
        self.write_set = write_set
        self.timestamp = timestamp
        self.status = "R"

    def __hash__(self):
        return self.tid.__hash__()
