import struct
import select
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
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # TODO needs to be assigned like the broadcaster
        self.sock.bind((socket.gethostbyname(socket.gethostname()), port))
        # TODO fill in boadcaster parameters. I dont know how the cluser params
        # work
        self.atomic_b = AtomicBroadcaster(10, ['TODO'], 10)

        self.__r_thread = th.Thread(target=self.__reconf_recv_worker())
        self.__r_thread.start()
        self.__l_thread = th.Thread(target=self.__list_recv_worker(), args=())
        self.__l_thread.start()


    def send_reconfigure(self):
        """ Sends a reconfigure request for the group consisting of the id """
        msg = struct.pack(recon_fmt, os.get_pid())
        self.atomic_b.broadcast(msg)

    def __reconf_recv_worker(self):
        # TODO there is 2 cases, we wait until there is an item in the list
        # or the period is over gonna use a semaphore i think just need to
        # decide best way to put it in the messagelist peek every time
        # semaphore is upped and if time will be in period pop
        # TODO Naeively wait until end of period before checking messages
        msg = None
        while True:
            remaining_t = time.time() % self.period
            msg = self.atomic_b.wait_for_message(remaining_t)
            if msg is None:
                self.last_group = self.cur_group
                self.cur_group = list()
            msg = struct.unpack('i', msg)
            #TODO fix
            self.add_host(msg[0])
        pass

    def __list_recv_list_worker(self):
        """ Waits for an attendance list, if none arrive by period
        requests new group to be formed. However if one is received forward the
        list to the next host """
        data = None
        msg = None
        while True:
            readable, _, _ = select.select([self.sock], [], [], timeout=self.period)
            for sock in readable:
                data = sock.recvfrom(104)
                msg = struct.unpack(list_fmt, data)
                members = list()
                num_items = msg[0]
                for i in range(num_items):
                    members.append(msg[i+1]) # basically umarshal the attendance list
                for i in range(len(members), 25): # 25 comes form struct list format
                    members.append(-1)
                self.forward_list(num_items, members)


        # we can either use select or socket.settimeout() in order to wait
        # period time to receive a list before issuing reconfiguration request
    def forward_list(self, num_items, members):
        msg = struct.pack(lst_fmt, num_items, *members)
        self.sock.sendto(msg, ('TODO put next host here', self.port))
