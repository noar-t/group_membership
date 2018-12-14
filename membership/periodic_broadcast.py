import struct
import time
import os
import threading as th
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster
from membership import LOG

class PeriodicBroadcastGroup(object):

    msg_fmt = '?di' # new-group(t/f), groupid, id

    def __init__(self, broadcaster, host, period=5):
        self.past_members = list()
        self.cur_members = list()

        self.cur_group = None
        self.cur_period = 0
        self.host = host #TODO ip?
        self.period = period
        # self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)
        self.atomic_b = broadcaster

        LOG.info("test2")
        self.__b_thread = th.Thread(target=self.__broadcast_worker)
        LOG.info("test3")
        self.__b_thread.start()
        LOG.info("test5")

    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.past_members


    def __broadcast_worker(self):
        """ Broadcasts present every period time units """
        # group should be V + pi because of reconfiguration latency
        # create new group upon initialization
        self.cur_group = time.time() + self.atomic_b.sigma
        self.send_broadcast(new_group=True)
        self.send_broadcast()

        # Processs messages and broadcast present
        while True:
            LOG.info("test1")
            timeout = self.period - ((time.time() - self.cur_group) % self.period)
            LOG.info("waiting timeout %f", timeout)
            msg = self.atomic_b.wait_for_msg(timeout)
            LOG.info("after waiting")
            # if there were no messages, the period is over
            if msg is None:
                LOG.info("waiting %s", self.get_members())
                self.past_members = self.cur_members
                self.cur_members = [self.host.id]
                self.cur_period += 1
            else:
                LOG.info("in else")
                self.msg_handler(msg)
            self.send_broadcast()

    def send_broadcast(self, new_group=False):
        """ Broadcast a message to all hosts """
        msg = struct.pack(self.msg_fmt, new_group, \
                          self.cur_group, self.host.id)
        self.atomic_b.broadcast(msg)

    def msg_handler(self, msg):
        """ Handle receipt of broadcasts """
        msg = struct.unpack(self.msg_fmt, msg)
        # if on time; myclock > V abort
        if msg[1] < time.time():
            # if new-group
            if msg[0]:
                LOG.info("new group requested")
                self.cur_group = msg[1]
                self.cur_period = 0
                self.cur_members = [self.host.id]
                self.past_members = list()

            # present broadcast
            else:
                LOG.info("member added %d", msg[3])
                # put the member in the group
                self.cur_members.append(msg[3])
