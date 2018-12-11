import time
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

class MessageList(object):
    #intermal format should be (time to accept message, message)
    #output list should just be message objects

    def __init__(self):
        self.messages = list()

    def get_messages(self):
        time = time.time()
        out = list()
        last_index = 0
        for i, message in enumerate(self.messages):
            # if message is ready to be received
            if message[0] > time:
                out.append(message[1])
                last_index = i+1
            else:
                break
        self.messages = self.messages[last_index:]
        pass

    def add_message(self, new_message):
        for i, message in enumerate(self.messages):
            if new_message[0] < message.time[0]:
                self.messages.insert(i, new_message)

