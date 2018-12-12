import socket
import time
import struct
import multiprocessing as mp
from membership import LOG


class Channel(object):

    def __init__(self, server_port, chan_id, out_queue):
        """ Initializes a Channel, which is essential a multicast group.
        Also creates a process which will listen on the channel for
        incoming channel messages.

        Keyword arguments:
        server_port      -- the server's port that this channel is running on
        out_queue -- a multiprocessing queue to place incoming messages
        """
        self.server_port = server_port
        self.channel_id = chan_id
        self.channel_port = server_port + chan_id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.msg_queue = out_queue
        self.__listener = mp.Process(target=self.__recv_worker, daemon=True)

        # bind before starting worker thread so we don't race in unittest
        LOG.debug(socket.gethostbyname(socket.gethostname()))
        LOG.info('channel %i created listening on port %s', self.channel_id,
                  self.channel_port)
        self.socket.bind((socket.gethostbyname(socket.gethostname()),
            self.channel_port))
        self.__listener.start()

    def send(self, ip, port, message):
        """ Send a message to dest connected to the channel """
        LOG.debug("sending message %s to %s at port %i", message, ip, port)
        message.time = time.time()
        self.socket.sendto(message.marshal(), (ip, port))

    def __recv_worker(self):
        """ Recieves messages and places them in the output queue """
        #messages are sized due to struct layout
        LOG.debug("receiving message")
        data, _ = self.socket.recvfrom(1080)
        LOG.debug("received message: %s", data)
        self.msg_queue.put((time.time(), data))
        while data:
            data, _ = self.socket.recvfrom(1080)
            self.msg_queue.put((time.time(), data))

class Message(object):
    def __init__(self, host, data, chan, copy=None):
        if copy is None:
            self.time = None #float
            self.hops = 1 #int
            self.host = host #should be parsed to 15 char array
            self.chan = chan
            self.data = data #1024 byte buffer
        else:
            #copy constructor/unmarshal
            self.unmarshal(data)

    def marshal(self):
        """ Turns a message object into an transmittable format """
        msg = struct.pack('fi15si1024s', self.time, \
                          self.hops, self.host, self.chan, self.data)

        return msg

    def unmarshal(self, data):
        """ Turns a recieved message back into a message object """
        msg = struct.unpack('fi15si1024s', data)
        self.time = msg[0]
        self.hops = msg[1]
        self.host = msg[2]
        self.chan = msg[3]
        self.data = msg[4]

    def add_hop(self):
        self.hops += 1

    def is_timely(self, time, sigma):
        """ Determines if a message is Timely U < T +h(δ+ε) """
        return time < (self.time + self.hops * sigma)
