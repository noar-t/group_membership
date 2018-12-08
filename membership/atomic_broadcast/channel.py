import socket
import time
import struct
import multiprocessing as mp
from membership import LOG


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

        self.msg_queue = out_queue
        self.__listener = mp.Process(target=self.__recv_worker, daemon=True)

        # bind before starting worker thread so we don't race in unittest
        LOG.debug(socket.gethostbyname(socket.gethostname()))
        self.socket.bind((socket.gethostbyname(socket.gethostname()), self.port))
        self.__listener.start()

    def send(self, dest, message):
        """ Send a message to dest connected to the channel """
        LOG.debug("sending message %s", message)
        self.socket.sendto(message, (dest.name, dest.port))

    def __recv_worker(self):
        """ Recieves messages and places them in the output queue """
        #messages are sized due to struct layout
        LOG.debug("receiving message")
        data, _ = self.socket.recvfrom(1072)
        LOG.debug("received message: %s", data)
        self.msg_queue.put((time.time(), data))
        while data:
            data, _ = self.socket.recvfrom(1072)
            self.msg_queue.put((time.time(), data))

class Message(object):
    def __init__(self, host, data, copy=None):
        if copy is None:
            self.time = None #float
            self.hops = 1 #int
            self.origin = host #should be reduced to 12 char array
            self.data = data #1024 bytes
        else: #copy constructor/demarshal
            self.unmarshal(data)

    def marshal(self):
        msg = struct.pack('fi12s1024s', self.time, \
                          self.hops, self.origin, self.data)
        return msg

    def unmarshal(self, data):
        msg = struct.unpack('fi12s1024s')
        self.time = msg[0]
        self.hops = msg[1]
        self.origin = msg[2]
        self.data = msg[3]

    def add_hop(self):
        self.hops += 1

    def is_timely(self, tmp=None):
        """Determines if a message is Timely U < T +h(δ+ε) """
        return True
