from multiprocessing import Process

from app import spawn
from start import start
from stop import stop

if __name__ == '__main__':
    sys_base = "multiprocTCPBase"
    # sys_base = "simpleSystemBase"
    start(sys_base)
    procs = []

    for i in range(0, 5):
        proc = Process(target=spawn, args=(sys_base, i+1))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()

    stop(sys_base)
