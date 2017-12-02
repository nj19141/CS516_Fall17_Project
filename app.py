import copy
import random

from thespian.actors import ActorExitRequest, ActorSystem

from thor.actors import DirectoryServer
from thor.clerk import Clerk
from thor.utils import Aggregator
import logging

KEYSPACE = 100

def spawn(sys_base, app_id):
    asys = ActorSystem(sys_base)
    clerk = asys.createActor(Clerk, globalName="clerk-%d" % app_id)
    asys.ask(clerk,
             Clerk.View(
                 asys.createActor(
                     DirectoryServer, globalName="directory-server"),
                 KEYSPACE,
             ))

    oids = set()
    while len(oids) != 5:
        key_ = random.randint(1, KEYSPACE)
        if key_ in oids:
            continue
        oids.add(key_)

    trx = asys.ask(clerk, Clerk.Read(list(oids)))
    mods = random.sample(oids, 3)
    for mod in mods:
        trx.write_set[mod] = app_id
    logging.debug("Clients initialized")
    success = asys.ask(clerk, Clerk.Commit(trx))
    print(success)

    asys.tell(clerk, ActorExitRequest())


if __name__ == "__main__":
    spawn("multiprocTCPBase", 0)