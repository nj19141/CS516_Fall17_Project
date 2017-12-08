from thespian.actors import ActorSystem
import logging

def stop(sys_base):
    ActorSystem(sys_base).shutdown()


if __name__ == "__main__":
    stop("multiprocTCPBase")
    logging.debug("Processes Stopped")
