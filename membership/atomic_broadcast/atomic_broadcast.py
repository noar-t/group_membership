import multiprocessing as mp
from membership.atomic_broadcast.channel import Channel, Message


class Host(object):
    """ Holds information about a single host that can receive a message. It is
    a single channel running on a process"""

    def __init__(self, name, port):
        self.name = name
        self.port = port


class AtomicBroadcaster(object):

    def __init__(self, hosts, ports):
        self.msg_queue = mp.Queue()
        self.hosts = hosts
        self.channels = [Channel(port, self.msg_queue) for port in ports]
        self.sigma = 5 #TODO this is not accurate
        self.__forwarder = mp.Process(target=self.__forwarder_worker,
                                      daemon=True)
        self.__forwarder.start()

    #Forwards message (T,s,,h+1), on channels c+1,...,f+1-h
    def __forwarder_worker(self):
        msg = None
        while True:
            msg = self.msg_queue.get()
            if msg.is_timely(self.sigma):
                #broadcast on subset of channels
                pass

    def calc_sigma(self):
        """find average ping to all hosts"""
        pass


    # Send message on all channels
    def broadcast(self, message):
        for c in self.channels:
            for host in self.hosts:
                c.send(host, message)
