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

        # LOG.info('channel %i created listening on port %s', self.channel_id,
                 # self.channel_port)
        # bind before starting worker thread so we don't race in unittest
        self.socket.bind((socket.gethostbyname(socket.gethostname()),
                          self.channel_port))
        self.__listener.start()

    def send(self, ip, port, msg):
        """ Send a message to dest connected to the channel """
        msg.chan = self.channel_id
        dest_channel_port = port + self.channel_id

        # LOG.debug("sending M(%f, %s, m, %i)->c%i to %s:%i", msg.time,
                  # self.server_port, msg.hops, msg.chan, ip, port)
        self.socket.sendto(msg.marshal(), (ip, dest_channel_port))

    def __recv_worker(self):
        """ Recieves messages and places them in the output queue """
        # messages are sized due to struct layout

        while True:
            binary_msg, addr = self.socket.recvfrom(1056)
            # calculate the port of the server rather than the channel
            addr = (addr[0], 100 * (addr[1] // 100))
            msg = Message.from_binary_msg(binary_msg, addr)
            LOG.debug("received message from: %s on c%i", addr,
                      msg.chan)
            self.msg_queue.put(msg)


class Message(object):
    def __init__(self, host, data, chan, copy=False):
        if not copy:
            self.time = None  # float
            self.hops = 1  # int
            self.host = host  # should be parsed to 15 char array
            self.chan = chan
            self.data = data  # 1024 byte buffer

            self.recv_time = None
            self.addr = None
        else:
            # copy constructor/unmarshal
            self.unmarshal(data)

    @classmethod
    def from_binary_msg(cls, binary_msg, addr):
        """ Initialize Message from a binary blob """
        msg = cls(None, binary_msg, None, copy=True)
        msg.recv_time = time.time()
        # LOG.debug("setting times: %f %f", time.time(), msg.time)
        msg.addr = addr

        return msg

    def marshal(self):
        """ Turns a message object into an transmittable format """
        msg = struct.pack('di15si1024s', self.time,
                          self.hops, self.host, self.chan, self.data)

        return msg

    def unmarshal(self, data):
        """ Turns a recieved message back into a message object """
        msg = struct.unpack('di15si1024s', data)
        self.time = msg[0]
        self.hops = msg[1]
        self.host = msg[2]
        self.chan = msg[3]
        self.data = msg[4]

    def add_hop(self):
        self.hops += 1

    def is_late(self, k, sigma):
        # LOG.debug("is_late: %f < (%f + (%i + 1) * %i)",
        # self.recv_time, self.time, k, sigma)
        # LOG.debug("\n%f\n%f", self.recv_time, self.time + (k+1)*sigma)
        return self.recv_time >= self.time + (k + 1) * sigma

    def is_timely(self, sigma):
        """ Determines if a message is Timely U < T +h(δ+ε) """
        # LOG.debug("is_timely: %f < (%f + %i + %f)",
        # self.recv_time, self.time, self.hops, sigma)
        return self.recv_time < (self.time + self.hops * sigma)

    def get_timely_deadline(self, sigma):
        return self.time + self.hops * sigma
