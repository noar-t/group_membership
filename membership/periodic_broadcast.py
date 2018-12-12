import struct
import time
import threading as th
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster
from membership.atomic_broadcast.channel import Message

class PeriodicBroadcastGroup(object):

    msg_fmt = 'i15s' # pid and ip

    def __init__(self, all_hosts, host, period=5):
        self.all_hosts = all_hosts
        self.set_lock = th.Lock()
        # TODO might be good to use the set like a ping pong buffer
        # as in we build it and then let the user read it then once
        # the next set for the next period is built we swap the pointer.
        # So new set every period and the set we return will not need 
        # synchronization
        self.last_group = list()
        self.cur_group = self.last_group
        self.host = None #TODO ip?
        self.period = period
        # TODO fill in boadcaster parameters. I dont know how the cluser params
        # work
        self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)

        self.__b_thread = th.Thread(target=self.__broadcast_worker())
        self.__b_thread.start()
        self.__r_thread = th.Thread(target=self.__recv_worker())
        self.__r_thread.start()

    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.last_group


    def __broadcast_worker(self):
        """ Broadcasts present every period time units """
        while True:
            self.cur_period = time.time()
            time.sleep(self.period)
            self.send_present()

    def send_present(self):
        """ Broadcast a present message to all hosts in the group """
        # TODO manipulate hosts in atomic broadcaster to only broadcast
        # to relivant hosts
        msg = struct.pack(msg_fmt, os.get_pid(), self.host)
        self.atomic_b.broadcast(msg)

    def __recv_worker(self):
        """ Gets messages then creates the membership list """
        # TODO there is 2 cases, we wait until there is an item in the list
        # or the period is over gonna use a semaphore i think just need to
        # decide best way to put it in the messagelist
        # This will build the set locking around access, basically just 
        # summing up the broadcasted present messages for the period
        msg = None
        while True:
            remaining_t = time.time() % self.period
            msg = self.atomic_b.wait_for_message(remaining_t)
            if msg is None:
                self.last_group = self.cur_group
                self.cur_group = list()
            msg = struct.unpack('i15s', msg)
            self.cur_group.append(msg[0])

