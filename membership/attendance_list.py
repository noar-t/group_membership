import struct
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
        self.delta = broadcaster.delivery_delay
        self.sigma = 1 #TODO change

        # time for last list receipt
        self.last_list = -1
        self.scheduled_tasks = list()

        self.__r_thread = th.Thread(target=self.__recv_worker)
        self.__r_thread.start()

        time.sleep(1)
        if join:
            new_group_time = time.time() + self.delta
            self.send_new_group(new_group_time)



    def __recv_worker(self):
        """ Work loop for worker """
        while True:
            msg = self.atomic_b.wait_for_msg(None)
            self.msg_handler(msg)

    def msg_handler(self, msg):
        """ Handle received message """
        msg_size = struct.unpack('i', msg.data[0:4])[0]
        #LOG.debug("size: %i, json: %s", msg_size, msg.data[4:4+msg_size])
        msg_dict = json.loads(msg.data[4:4+msg_size])

        # if "new-group" received
        if 'new_group' in msg_dict:
            #can try if time.time > ['gid'] later
            self.members = set()
            for task in self.scheduled_tasks:
                task.cancel()
            self.send_present(msg_dict['gid'])
            check_task = th.Timer(msg_dict['gid'] + self.period,
                                  self.__membership_check,
                                  args=(msg_dict['gid'],))
            check_task.start()
            self.scheduled_tasks.append(check_task)

        elif 'present' in msg_dict:
            #TODO check correct group id
            LOG.info("Member %i added to %d", msg_dict['id'], msg_dict['gid'])
            self.members.add(msg_dict['id'])

        elif 'list' in msg_dict:
            #TODO check time < O and gamma
            self.last_list = time.time() #TODO this is O not 0 needs to be fixed
            if not host.id == max(self.members):
                self.send_list(msg_dict['members'])

    def send_new_group(self, t):
        """ Sends a reconfigure request for the group consisting of the id """
        msg_dict = {'new_group' : True,
                    'gid' : t }
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        LOG.debug("sending new_group: %i", t)
        self.atomic_b.broadcast(msg_size + msg_bytes)

    def send_present(self, t):
        msg_dict = {'present' : True,
                    'gid' : t,
                    'id' : self.host.id }
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        self.atomic_b.broadcast(msg_size + msg_bytes)

    def send_list(self, members):
        LOG.debug("host%i sending list %s", self.host.id, members)
        msg_dict = {'list' : True,
                    'gid' : self.cur_group,
                    'members' : members}
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        dest = self.get_next_host()
        msg = Message(None, msg_size + msg_bytes, -1)
        msg.hops = -1
        port = 50000 + (100 * dest) + 1
        # send on the first channel, abuse the system
        self.atomic_b.channels[0].sendto(msg.marshal(), ('localhost', port))

    def get_next_host():
        next_host = None
        for m in sorted(self.members):
            if m == self.host.id:
                next_host = True
            elif not next_host is None:
                next_host = m
                break
        return next_host


    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.members


    def __membership_confirmation(self, check_time):
        #TODO this is probably broken sends in delta check time instead of abs check time
        if time.time() > check_time:
            return
        if self.list_r_t + len(self.members)*self.sigma < check_time:
            self.send_new_group(time.time())


    def __membership_check(self, check_time):
        self.members.add(self.host.id)
        if self.host.id == max(self.members):
            self.send_list(self.members)
        gamma = len(self.members)*self.sigma
        confirm_time = check_time - time.time() + gamma
        confirm_task = th.Timer(confirm_time,
                                self.__membership_confirmation,
                                args=(check_time + gamma,))
        self.scheduled_tasks.append(confim_task)
        confirm_task.start()

        mem_check_time = check_time - time.time() + self.period
        mem_check_task = th.Timer(mem_check_time,
                                self.__membership_check,
                                args=(check_time + self.period,))
        self.scheduled_tasks.append(mem_check_task)
        mem_check_task.start()
