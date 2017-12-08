import copy
import logging
import random
import time

from thespian.actors import ActorExitRequest, ActorSystem

from thor.actors import DirectoryServer
from thor.clerk import Clerk
from thor.utils import Aggregator

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
    while len(oids) != 10:
        key_ = random.randint(1, KEYSPACE)
        if key_ in oids:
            continue
        oids.add(key_)
    
    success = False
    while not success:
        time.sleep(0.5)
        trx = asys.ask(clerk, Clerk.Read(list(oids)))
        if trx is False:
            continue
        mods = random.sample(oids, 5)
        for mod in mods:
            trx.write_set[mod] = app_id
        logging.debug("Clients initialized")
        success = asys.ask(clerk, Clerk.Commit(trx))
        print(success)

    asys.tell(clerk, ActorExitRequest())


if __name__ == "__main__":
    spawn("multiprocTCPBase", 0)
