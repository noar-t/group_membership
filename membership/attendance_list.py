import struct
import socket
import time
import threading as th
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster
from membership.atomic_broadcast.channel import Message

# TODO reconfiguration port must be differnt than typical broadcast port because
# I dont want to have to identify type of messages
# TODO how do i map pid to port/ip because reconfiguration broadcasts will be
# just a pid
class AttendanceListGroup(object):

    recon_fmt = 'i' # only used for reconfiguration
    list_fmt = 'i25i' # # of elements, actual list

    def __init__(self, host,  port, period=5,leader=None):
        self.set_lock = th.Lock()
        self.cur_group = set()
        self.cur_period = None
        self.host = None # TODO ip?
        self.period = period
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # TODO needs to be assigned like the broadcaster
        self.sock.bind((socket.gethostbyname(socket.gethostname()), port))
        # TODO fill in boadcaster parameters. I dont know how the cluser params
        # work
        self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)

        #self.__b_thread = th.Thread(target=self.__forward_worker(), args=())
        #self.__b_thread.start()
        self.__r_thread = th.Thread(target=self.__reconf_recv_worker())
        self.__r_thread.start()


    def send_reconfigure(self):
        """ Sends a reconfigure request for the group consisting of the id """
        msg = struct.pack(recon_fmt, os.get_pid())
        self.atomic_b.broadcast(msg)

    def __reconf_recv_worker(self):
        # TODO there is 2 cases, we wait until there is an item in the list
        # or the period is over gonna use a semaphore i think just need to
        # decide best way to put it in the messagelist
        pass

    def __list_recv_worker(self):
        """ Waits for an attendance list, if none arrive by period
        requests new group to be formed. However if one is received forward the
        list to the next host """
        # we can either use select or socket.settimeout() in order to wait
        # period time to receive a list before issuing reconfiguration request
        pass
