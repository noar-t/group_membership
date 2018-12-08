import multiprocessing as mp
from membership.atomic_broadcast.channel import Channel, Message
from membership import LOG


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
        self.channels = [Channel(port, chan_id, self.msg_queue) \
                         for chan_id, port in enumerate(ports)]
        #TODO this is not accurate, need to flushout calc_sigma
        self.sigma = 5
        self.__forwarder = mp.Process(target=self.__forwarder_worker,
                                      daemon=True)
        self.__forwarder.start()

    #Forwards message (T,s,,h+1), on channels c+1,...,f+1-h
    def __forwarder_worker(self):
        msg = None
        recv_time = None
        while True:
            recv_time, msg = self.msg_queue.get()
            msg = Message(None, msg, True)
            if msg.is_timely(recv_time, self.sigma):
                #broadcast on subset of channels
                pass

    def calc_sigma(self):
        """ Find average ping to all hosts """
        pass


    # Send message on all channels
    def broadcast(self, message):
        for c in self.channels:
            for host in self.hosts:
                c.send(host, message)
