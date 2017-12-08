import logging

from thespian.actors import ActorSystem

from thor.actors import DirectoryServer, Server

KEY_SPACE = 100

logcfg = {
    'version': 1,
    'formatters': {
        'normal': {
            'format': '%(levelname)-8s %(message)s'
        }
    },
    'handlers': {
        'h': {
            'class': 'logging.FileHandler',
            'filename': 'thor.log',
            'formatter': 'normal',
            'level': logging.DEBUG
        }
    },
    'loggers': {
        '': {
            'handlers': ['h'],
            'level': logging.DEBUG
        }
    }
}


def _group_by_server(server_map_: dict) -> dict:
    answer = {}
    for oid, server in server_map_.items():
        if server not in answer:
            answer[server] = set()
        answer[server].add(oid)
    logging.debug("Server map obtained")
    return answer


def start(sys_base):
    asys = ActorSystem(sys_base, logDefs=logcfg)
    ds = asys.createActor(DirectoryServer, globalName="directory-server")
    db_servers = {}
    for i in range(0, 5):
        name = "db-server-%d" % i
        db_servers[name] = asys.createActor(Server, globalName=name)

    server_map = {
        key: "db-server-%d" % (key % len(db_servers))
        for key in range(1, KEY_SPACE + 1)
    }
    asys.ask(ds, DirectoryServer.WhoServes(server_map))
    grouped_server_map = _group_by_server(server_map)
    for server_name, key_set in grouped_server_map.items():
        asys.ask(db_servers[server_name], Server.View(key_set))


if __name__ == "__main__":
    start("multiprocTCPBase")
