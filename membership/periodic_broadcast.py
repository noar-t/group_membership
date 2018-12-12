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
        self.cur_group = set()
        self.cur_period = None
        self.host = None #TODO ip?
        self.period = period
        # TODO fill in boadcaster parameters. I dont know how the cluser params
        # work
        self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)

        self.__b_thread = th.Thread(target=self.__broadcast_worker())
        self.__b_thread.start()
        self.__r_thread = th.Thread(target=self.__recv_worker())
        self.__r_thread.start()

    def __broadcast_worker(self):
        while True:
            self.cur_period = time.time()
            time.sleep(self.period)
            self.send_present()

    def send_present(self):
        msg = struct.pack(msg_fmt, os.get_pid(), self.host)
        self.atomic_b.broadcast(msg)

    def __recv_worker(self):
        # TODO there is 2 cases, we wait until there is an item in the list
        # or the period is over gonna use a semaphore i think just need to
        # decide best way to put it in the messagelist
        pass
        
