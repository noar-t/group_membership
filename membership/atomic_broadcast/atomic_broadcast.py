import time
import threading as th
import multiprocessing as mp
from membership.atomic_broadcast.channel import Channel, Message
from membership import LOG


class AtomicBroadcaster(object):

    def __init__(self, server_port, hosts, channel_count):
        """
        :server_port: port of the server that this AtomicBroadcaster is running
        on
        :hosts: dict of hosts running in the cluster
        :channel_count: number of channels to create
        """
        self.msg_queue = mp.Queue()
        self.hosts = hosts
        self.server_port = server_port
        self.channels = [Channel(server_port, n + 1, self.msg_queue)
                         for n in range(channel_count)]
        LOG.info("atomicbroadcaster created")
        LOG.info("hosts: %s", self.hosts)
        # TODO this is not accurate, need to flushout calc_sigma
        self.sigma = 5
        LOG.info("atomicbroadcaster created")
        self.message_list = MessageList()
        self.__forwarder = th.Thread(target=self.__forwarder_worker)
        self.__forwarder.start()

        # XXX
        # m = Message(b'123451234512345', b'HI', 0)

        # time.sleep(2)
        # self.broadcast(m)

    # Forwards message (T,s,,h+1), on channels c+1,...,f+1-h
    def __forwarder_worker(self):
        msg = None
        recv_time = None
        while True:
            recv_time, binary_msg = self.msg_queue.get()
            msg = Message(None, binary_msg, None, True)
            if msg.is_timely(recv_time, self.sigma):
                self.message_list.add_message(accept_time, msg)
                self.__forward(msg)

    def __forward(self, msg):
        LOG.info("forwarding msg from %i", self.server_port)
        c = msg.chan
        h = msg.hops
        msg.add_hop()
        for channel_id in range(c+1, len(self.channels)):
            chan = self.channels[channel_id]
            for _, host in self.hosts.items():
                chan.send(host.ip, host.port, msg)

    def calc_sigma(self):
        """ Find average ping to all hosts """
        # TODO we can use popen to invoke ping but that may be poor style
        pass

    def get_messages(self):
        return self.message_list.get_messages()

    # Send message on all channels
#<<<<<<< HEAD
#    def broadcast(self, message):
#        message_out = None
#        for c in self.channels:
#            #TODO need to get host ip for first argument
#            message_out = Message(None, message, c.channel_id)
#            for _, host in self.hosts.items():
#                c.send(host.ip, host.port, message_out)
#=======
    def broadcast(self, msg):
        for channel in self.channels:
            for _, host in self.hosts.items():
                channel.send(host.ip, host.port, msg)

#>>>>>>> b00f0a934aa847ca54717142faed4367cf0f1fe5

class MessageList(object):
    #intermal format should be (time to accept message, message)
    #output list should just be message objects

    def __init__(self):
        self.messages = list()
        self.lock = th.Lock()

    def get_messages(self):
        t = time.time()
        out = list()
        last_index = 0
        self.lock.aqcuire()
        time = time.time()
        for i, message in enumerate(self.messages):
            # if message is ready to be received
            if message[0] > t:
                out.append(message[1])
                last_index = i+1
            else:
                break
        self.messages = self.messages[last_index:]
        self.lock.release()
        return out

    def add_message(self, accept_time, new_message):
        self.lock.aqcuire()
        for i, message in enumerate(self.messages):
            if accept_time < message.time[0]:
                self.messages.insert(i, (accept_time, new_message))
        self.lock.release()
