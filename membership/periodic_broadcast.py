import struct
import time
import os
import threading as th
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster

class PeriodicBroadcastGroup(object):

    msg_fmt = '?di15s' # new-group(t/f), groupid, pid and ip; 35 bytes

    def __init__(self, all_hosts, host, period=5):
        self.past_members = list()
        self.cur_members = list()

        self.cur_group = None
        self.cur_period = 0
        self.host = None #TODO ip?
        self.period = period
        self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)

        self.__b_thread = th.Thread(target=self.__broadcast_worker())
        self.__b_thread.start()

    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.past_members


    def __broadcast_worker(self):
        """ Broadcasts present every period time units """
        # group should be V + pi because of reconfiguration latency
        # create new group upon initialization
        self.cur_group = time.time() + self.period
        self.send_broadcast(new_group=True)

        # Processs messages and broadcast present
        while True:
            timeout = self.period - ((time.time() - self.cur_group) % self.period)
            msg = self.wait_for_message(timeout)
            # if there were no messages, the period is over
            if msg is None:
                self.past_members = self.cur_group
                #TODO do I put me in here
                self.cur_members = list()
            else:
                self.msg_handler(msg)
            self.send_broadcast()
            self.cur_period += 1

    def send_broadcast(self, new_group=False):
        """ Broadcast a message to all hosts """
        msg = struct.pack(self.msg_fmt, new_group, \
                          self.cur_group, os.getpid(), self.host)
        self.atomic_b.broadcast(msg)

    def msg_handler(self, msg):
        """ Handle receipt of broadcasts """
        t = time.time()
        rem_t = ((msg[0] - self.cur_group) - (self.period * self.cur_period))
        # received a message in the form form (delivery time, msg)
        # message not ready to be delivered but will be this period
        if t < msg[0] and rem_t < self.period:
            self.msg_handler(self.wait_for_message(rem_t))
            # TODO make better abstraction
            msg =  self.atomic_b.message_list.pop()

        msg = struct.unpack(self.msg_fmt, msg[1])
        # if on time; myclock > V abort
        if msg[1] < time.time():
            # if new-group
            if msg[0]:
                self.cur_group = msg[1]
                self.cur_period = 0
                self.cur_members = [(msg[2], msg[3])]
                self.past_members = list()

            # present broadcast
            else:
                # put the member in the group
                self.cur_members.append((msg[2], msg[3]))

    def wait_for_message(self, timeout):
        """ Gets messages blocking for a period """ 
        # calculate time left in current period
        return self.atomic_b.wait_for_message(timeout)


