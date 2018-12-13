import struct
import select
import socket
import json
import time
import os
import threading as th
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster
from membership.atomic_broadcast.channel import Message

class AttendanceListGroup(object):

    def __init__(self, broadcaster, host, period=5):
        self.cur_members = list()
        self.past_members = list()
        self.cur_group = 0

        self.period = period
        self.cur_period = 0
        self.list_recv = False

        self.host = host

        # self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)
        self.atomic_b = broadcaster

        self.__r_thread = th.Thread(target=self.__recv_worker)
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
            # If first host start list propogation
            if self.cur_period > 0:
                if self.host.id == min(self.cur_members):
                    self.forward_list(list())

            remaining_t = time.time() % self.period
            LOG.debug("remaining %f", remaining_t)
            msg = self.atomic_b.wait_for_message(remaining_t)

            # period is over
            if msg is None:
                LOG.debug("cur memeber %s", self.cur_members)
                # TODO fix reconfigure on first period due to no list also add 'leader'
                if not self.list_recv and not self.cur_period == 0:
                    self.send_reconfigure()
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
                self.cur_group =  msg_dict['gid']
                self.past_members = list()
                self.cur_members = [self.host.id]

            elif 'present' in msg_dict:
                # present message TODO check group id matches
                if msg_dict['gid'] == self.cur_group:
                    self.cur_members.append(msg['id'])

            # forward list only if its current group
            elif 'list' in msg_dict:
                if msg_dict['gid'] == self.cur_group:
                    self.list_recv = True
                    if self.host.id == max(self.cur_members):
                        self.forward_list(msg_dict['members'])



        # we can either use select or socket.settimeout() in order to wait
        # period time to receive a list before issuing reconfiguration request
    def forward_list(self, members):
        msg_dict = {'list' : True,
                    'gid' : self.cur_group,
                    'members' : members.append(self.host.id)}
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


