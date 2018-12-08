# Since there is no access to creating and setting up multicast groups on
# UTCS routers we will spew the UDP packets to all hosts manually
import socket
import time
import multiprocessing as mp
# from atomic_broadcast import Host


class Channel(object):

    def __init__(self, port, out_queue):
        """Initializes a Channel, which is essential a multicast group.
        Also creates a process which will listen on the channel for
        incoming channel messages.

        Keyword arguments:
        port      -- the port which to send out the udp messages to
        out_queue -- a multiprocessing queue to place incoming messages
        """
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('LOCALHOST', self.port))

        # os.inhertiable?

        self.msg_queue = out_queue
        self.__listener = mp.Process(target=self.__recv_worker, daemon=True)
        self.__listener.start()

    def send(self, dest, message):
        """ Send a message to dest connected to the channel """
        print('sending', dest.__dict__)
        self.socket.sendto(message, (dest.name, dest.port))

    # def get_message(self):
       # """ Block until a message is received """
       # return self.msg_queue.get()

    # @property
    # def queue(self):
       # return self.msg_queue

    # TODO time stamp the msg immediately then check timelyness before sending
    # TODO Put msg into queue immediately
    def __recv_worker(self):
        """ Recieves messages and places them in the output queue """
        data, _ = self.socket.recvfrom(1024)
        self.msg_queue.put((time.time(), data))
        while data:
            data, _ = self.socket.recvfrom(1024)
            self.msg_queue.put((time.time(), data))


class Message(object):
    def __init__(self, host, data):
        self.time = None
        self.hops = 1
        self.origin = host
        self.data = data

    def add_hop(self):
        self.hops += 1

    def is_timely(self, tmp=None):
        pass
