import struct
import select
import socket
import json
import time
import os
import threading as th
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster

class AttendanceListGroup(object):
    #recon_fmt = '??di15s' # reconfig(y/n), present (y/n), new group id, host id, ip
    #list_fmt = '?id25i15s' # reconfig(y/n), # of elements, group_id, actual list, ip

    def __init__(self, host,  port, period=5):
        self.set_lock = th.Lock()
        self.cur_group = list()

        self.period = period
        self.cur_period = 0

        self.host = host # TODO ip?
        self.next_host = None

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
        #msg = struct.pack(self.recon_fmt, True, False, t, os.getpid())
        #self.atomic_b.broadcast(msg)
        ## present message
        msg_dict = {'present' : False,
                    'gid' : t,
                    'id' : self.host.id }
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        self.atomic_b.broadcast(msg_size + msg_bytes)
        #msg = struct.pack(self.recon_fmt, True, True, t, os.getpid())
        #self.atomic_b.broadcast(msg)

    def __recv_worker(self):
        # RECONFIGURE UPON INIT
        self.send_reconfigure()
        msg = None

        while True:
            remaining_t = time.time() % self.period
            msg = self.atomic_b.wait_for_message(remaining_t)

            # period is over
            if msg is None:
                self.last_group = self.cur_group
                # TODO do I put me in there
                self.cur_group = list()
            else:
                self.msg_handler(msg)

    def msg_handler(self, msg):
        """ Handle receipt of broadcasts """
        t = time.time()
        # received a message in the form form (delivery time, msg)
        msg_t = msg[0]
        rem_t = ((msg_t - self.cur_group) - (self.period * self.cur_period))
        msg = json.loads(msg[1].decode())

        # message not ready to be delivered but will be this period
        if t < msg_t and rem_t < self.period:
            self.msg_handler(self.atomic_b.wait_for_message(rem_t))
            # This might be racey
            msg =  self.atomic_b.message_list.pop()

        msg_size = struct.unpack('i', msg[1][0:4])
        msg_dict = json.loads(msg[1][4:5+msg_size[0]])
        # if on time; myclock > V abort
        if msg_dict['gid'] < time.time():
            # if reconfigure
            if msg[0]: #TODO fix struct to have time stamp within
                # present message TODO check group id matches
                msg = msg_tmp
                if msg[1]:
                    self.cur_members.append((msg[3], msg[4]))
                # just a new group
                else:
                    # TODO change cur period, group name, cancel any list
                    # receives in the period
                    self.cur_period = 0
                    self.cur_group =  msg[2]
                    self.past_members = list()
                    self.cur_members = list()

            # list message
            else:
                msg = struct.unpack(self.list_fmt, msg[1])
                # put the member in the group
                # TODO forward list; need to unpack list
                # check group id
                self.forward_list(None, None)

    #def __list_recv_list_worker(self):
    #    """ Waits for an attendance list, if none arrive by period
    #    requests new group to be formed. However if one is received forward the
    #    list to the next host """
    #    # TODO we can do some shit and just send it to an existing channel for the 
    #    # next process. This solves the issue of waiting on multiple things. We can
    #    # just wait on the Message list
    #    data = None
    #    msg = None
    #    while True:
    #        readable, _, _ = select.select([self.sock], [], [], timeout=self.period)
    #        for sock in readable:
    #            data = sock.recvfrom(104)
    #            msg = struct.unpack(list_fmt, data)
    #            members = list()
    #            num_items = msg[0]
    #            for i in range(num_items):
    #                members.append(msg[i+1]) # basically umarshal the attendance list
    #            for i in range(len(members), 25): # 25 comes form struct list format
    #                members.append(-1)
    #            self.forward_list(num_items, members)


        # we can either use select or socket.settimeout() in order to wait
        # period time to receive a list before issuing reconfiguration request
    def forward_list(self, num_items, members):
        msg = struct.pack(lst_fmt, num_items, *members)
        self.sock.sendto(msg, ('TODO put next host here', self.port))
