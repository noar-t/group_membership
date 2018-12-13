import struct
import select
import socket
import json
import time
import os
import threading as th
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster
from membership.atomic_broadcast.channel import Message

# TODO I think the group id needs to have a offset otherwise the message will be rejected
# TODO make 'leader' to schedulue list send initiation
class AttendanceListGroup(object):

    def __init__(self, host, period=5):
        self.cur_members = list()
        self.past_members = list()
        self.cur_group = 0

        self.period = period
        self.cur_period = 0

        self.host = host

        self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)

        self.__r_thread = th.Thread(target=self.__recv_worker())
        self.__r_thread.start()


    def send_reconfigure(self):
        """ Sends a reconfigure request for the group consisting of the id """
        # reconfigure message
        t = time.time()
        msg_dict = {'recon' : True,
                    'gid' : t }
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        self.atomic_b.broadcast(msg_size + msg_bytes)
        # present message
        msg_dict = {'present' : True,
                    'gid' : t,
                    'id' : self.host.id }
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        self.atomic_b.broadcast(msg_size + msg_bytes)

    def __recv_worker(self):
        # RECONFIGURE UPON INIT
        self.send_reconfigure()
        msg = None

        while True:
            remaining_t = time.time() % self.period
            msg = self.atomic_b.wait_for_message(remaining_t)

            # period is over
            if msg is None:
                if not self.cur_period == 0:
                    self.send_ping()
            else:
                self.msg_handler(msg)

    def msg_handler(self, msg):
        """ Handle receipt of broadcasts """
        t = time.time()
        # received a message in the form form (delivery time, msg)
        msg_t = msg[0]
        rem_t = ((msg_t - self.cur_group) - (self.period * self.cur_period))

        # message not ready to be delivered but will be this period
        if t < msg_t and rem_t < self.period:
            self.msg_handler(self.atomic_b.wait_for_message(rem_t))
            # This might be racey
            msg = self.atomic_b.message_list.pop()

        msg_size = struct.unpack('i', msg[1][0:4])
        msg_dict = json.loads(msg[1][4:5+msg_size[0]])
        # if on time; myclock > V abort
        if msg_dict['gid'] <= time.time():
            # if reconfigure
            if 'recon' in msg_dict:
                # Join the new group
                self.cur_period = 0
                self.cur_group = msg_dict['gid']
                self.past_members = list()
                self.cur_members = [self.host.id]

            elif 'present' in msg_dict:
                # present message TODO check group id matches
                if msg_dict['gid'] == self.cur_group:
                    self.cur_members.append(msg['id'])

            # forward list only if its current group
            elif 'ping' in msg_dict:
                if msg_dict['gid'] == self.cur_group:
                    # TODO respond to ping also need to set somethign
                    # for reconfiguration if both prev and next host dont ping

    def send_ping(self):
        msg_dict = {'ping' : True,
                    'gid' : self.cur_group,
                    'sender' : self.host.id}
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        dest = self.get_next_host()
        msg = Message(None, msg_size + msg_bytes, -1)
        msg.hops = -1
        port = 50000 + (100 * dest) + 1
        # send on the first channel, abuse the system
        self.atomic_b.channels[0].sendto(msg.marshal(), ('localhost', port))

    def get_next_host(self):
        my_id = self.host.id
        next_host = 999999
        for h_id in self.cur_members:
            if h_id > my_id and h_id < next_host:
                next_host = h_id
        return next_host


