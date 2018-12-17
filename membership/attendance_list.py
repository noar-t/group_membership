import struct
import socket
import time
import json
import threading as th
from membership import LOG
from membership.atomic_broadcast.channel import Message


class AttendanceListGroup(object):

    def __init__(self, broadcaster, host, join, period=10):
        self.host = host
        self.atomic_b = broadcaster
        self.period = period

        self.members = set()
        self.present_members = set()
        self.delta = broadcaster.delivery_delay
        self.sigma = 1  #TODO change
        # self.scheduled_broadcasts = {}

        # time for last list receipt
        self.last_r_t = -1
        self.scheduled_tasks = list()
        self.group = None

        self.__r_thread = th.Thread(target=self.__recv_worker)
        self.__r_thread.start()

        time.sleep(2)
        if join:
            new_group_time = time.time() + self.delta
            self.cur_group = new_group_time
            # self.send_broadcast(new_group_time, new_group=True)
            self.send_new_group(new_group_time)
            self.present_members.add(self.host.id)

            confirm_time = new_group_time + 2 * self.delta
            confirm_task = th.Timer(confirm_time - time.time(),
                                    self.__new_group_confirmation,
                                    args=(new_group_time,))
            # self.scheduled_broadcasts[confirm_time] = confirm_task
            self.scheduled_tasks.append(confirm_task)
            confirm_task.start()

    def __recv_worker(self):
        """ Work loop for worker """
        while True:
            msg = self.atomic_b.wait_for_msg(None)
            self.msg_handler(msg)

    def msg_handler(self, msg):
        """ Handle received message """
        msg_size = struct.unpack('i', msg.data[0:4])[0]
        #LOG.debug("size: %i, json: %s", msg_size, msg.data[4:4+msg_size])
        # LOG.debug("host%i recieved msg: %s", self.host.id, msg_dict)
        msg_dict = json.loads(msg.data[4:4 + msg_size].decode())

        # if "new-group" received
        if 'new_group' in msg_dict:
            # cancel membership_check and membership_confirmation tasks
            for task in self.scheduled_tasks:
                task.cancel()
            self.scheduled_tasks = []
            #can try if time.time > ['gid'] later
            LOG.info("%i: new_group %f, from %i", self.host.id, msg_dict['gid'], msg_dict['id'])
            self.members = set()
            self.group = msg_dict['gid']
            self.send_present(msg_dict['gid'])
            LOG.debug("timer for %f", msg_dict['gid'] - time.time() + self.period)
            self.present_members.add(self.host.id)
            self.present_members.add(msg_dict['id'])
            # confirm_time = new_group_time + 2 * self.delta
            wait_t = msg_dict['gid'] + 2 * self.delta - time.time()
            confirm_task = th.Timer(wait_t,
                                    self.__new_group_confirmation,
                                    args=(msg_dict['gid'],))
            # self.scheduled_broadcasts[confirm_time] = confirm_task
            self.scheduled_tasks.append(confirm_task)
            confirm_task.start()

            # check_task = th.Timer(msg_dict['gid'] - time.time() + self.period,
                                  # self.__membership_check,
                                  # args=(msg_dict['gid'],))
            # check_task.start()
            # self.scheduled_tasks.append(check_task)

        elif 'present' in msg_dict:
            #TODO check correct group id
            LOG.info("%i: present %f, from %i", self.host.id, msg_dict['gid'], msg_dict['id'])
            # self.members.add(msg_dict['id'])
            self.present_members.add(msg_dict['id'])

        elif 'list' in msg_dict:
            #TODO check time < O and gamma
            LOG.info("%i: list received", self.host.id)
            self.last_r_t = time.time()  #TODO this is O not 0 needs to be fixed
            if not self.host.id == max(self.members):
                self.send_list([self.host.id])

    def send_new_group(self, t):
        """ Sends a reconfigure request for the group consisting of the id """
        msg_dict = {'new_group': True,
                    'gid': t,
                    'id': self.host.id}
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        LOG.debug("host%i sending new_group: %f", self.host.id, t)
        # LOG.debug("host%i sending new_group: %s", self.host.id, msg_bytes)
        self.atomic_b.broadcast(msg_size + msg_bytes)

    # def __schedule_new_group_confirmation(self, t):
        # task = th.Timer(

    def __new_group_confirmation(self, check_time):
        LOG.info("\033[95 m%i: members arrived before confirm %s\033[0m",
                 self.host.id, self.present_members)
        if self.present_members != self.members:
            LOG.debug("confirming: UPDATING mems from %s to %s",
                      self.members, self.present_members)
            self.members = self.present_members
            # self.group += self.period
            # self.group = check_time
            self.present_members = set([self.host.id])
            self.check_members = set([self.host.id])
        else:
            LOG.debug("%i: confirming: OKAY mems at %s", self.host.id,
                      self.members)
            self.check_members = set([self.host.id])
        # XXX
        # for key, task in self.scheduled_broadcasts.items():
            # # if msg[0] <= key:
            # task.cancel()
        # self.scheduled_broadcasts = {}

        for task in self.scheduled_tasks:
            task.cancel()
        self.scheduled_tasks = []
        # self.__membership_check(check_time)
        next_check_time = check_time + self.period
        next_check_task = th.Timer(next_check_time - time.time(),
                                   self.__membership_check,
                                   args=(next_check_time,))
        # self.scheduled_broadcasts[next_check_time] = next_check_task
        self.scheduled_tasks.append(next_check_task)
        next_check_task.start()

        # schedule next check task; next check at check_time + period
        # next_check_time = check_time + self.period
        # next_check_task = th.Timer(next_check_time - time.time() - 1,
                                   # self.__membership_check_task,
                                   # args=(next_check_time,))
        # self.scheduled_broadcasts[next_check_time] = next_check_task
        # next_check_task.start()

    def send_present(self, t):
        msg_dict = {'present': True,
                    'gid': t,
                    'id': self.host.id}
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        self.atomic_b.broadcast(msg_size + msg_bytes)

    def send_list(self, members):
        LOG.debug("host%i sending list %s", self.host.id, members)
        msg_dict = {'list': True,
                    'gid': self.group,
                    'members': list(members)}
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        dest = self.get_next_host()
        if self.host.id == max(self.members):
            dest = min(self.members)
        msg = Message(None, msg_size + msg_bytes, -1)
        msg.hops = -1
        msg.time = 0
        msg.host = -1
        msg.chan = -1
        LOG.debug("host%i, sending list to host%i", self.host.id, dest)
        LOG.debug("members: %s", self.members)
        port = 50000 + (100 * dest) + 1
        # send on the first channel, abuse the system
        self.atomic_b.channels[0].socket.sendto(msg.marshal(),
                                                (socket.gethostbyname(socket.gethostname()), port))

    def get_next_host(self):
        next_host = None
        for m in sorted(self.members):
            if m == self.host.id:
                next_host = True
            elif next_host is not None:
                next_host = m
                break
        return next_host

    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.members

    def __membership_confirmation(self, check_time):
        #TODO this is probably broken sends in delta check time instead of abs check time
        LOG.debug("host%i: membership confirm task", self.host.id)
        #time.time() > check_time:
        #    return
        if self.last_r_t + len(self.members) * self.sigma + .1 < check_time:
            LOG.debug("%i: SENDING NEW GROUP!!!!!!!!!!", self.host.id)
            new_group_time = time.time() + self.delta
            self.send_new_group(time.time())
            # self.send_broadcast(check_time + self.delta, new_group=True)
            self.present_members.add(self.host.id)

            confirm_time = new_group_time + 2 * self.delta
            confirm_task = th.Timer(confirm_time - time.time(),
                                    self.__new_group_confirmation,
                                    args=(new_group_time,))
            # self.scheduled_broadcasts[confirm_time] = confirm_task
            self.scheduled_tasks.append(confirm_task)
            confirm_task.start()

    def __membership_check(self, check_time):
        LOG.debug("host%i membership check task", self.host.id)
        # self.members.add(self.host.id)
        if self.host.id == max(self.members):
            self.send_list([self.host.id])
        gamma = len(self.members) * self.sigma
        confirm_time = check_time - time.time() + gamma + .1
        confirm_task = th.Timer(confirm_time,
                                self.__membership_confirmation,
                                args=(check_time + gamma,))
        self.scheduled_tasks.append(confirm_task)
        confirm_task.start()

        mem_check_time = check_time - time.time() + self.period
        mem_check_task = th.Timer(mem_check_time,
                                  self.__membership_check,
                                  args=(check_time + self.period,))
        self.scheduled_tasks.append(mem_check_task)
        mem_check_task.start()
