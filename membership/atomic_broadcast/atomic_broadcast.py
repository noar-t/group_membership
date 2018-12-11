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
            #TODO should probably split logic up better
            if msg.is_timely(recv_time, self.sigma):
                c = msg.chan
                h = msg.hops
                msg = msg.add_hop()
                for i in range(c+1, len(self.channels)):
                    chan = self.channels[i]
                    for host in self.hosts:
                        msg.chan = i
                        chan.send(host, msg.marshal())

    def calc_sigma(self):
        """ Find average ping to all hosts """
        #TODO we can use popen to invoke ping but that may be poor style
        pass


    # Send message on all channels
    def broadcast(self, message):
        for c in self.channels:
            for host in self.hosts:
                c.send(host, message)
