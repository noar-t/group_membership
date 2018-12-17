import struct
import time
import threading as th
from membership import LOG


class AttendanceListGroup(object):

    def __init__(self, broadcaster, host, join, period=10):
        self.host = host
        self.atomic_b = broadcaster
        self.period = period

        self.members = set()
        self.delta = broadcaster.delivery_delay

        # time for last list receipt
        self.last_list = -1
        # also acts as joined boolean
        self.group = None


    def __recv_worker(self):
        """ Work loop for worker """
        while True:
            msg = self.atomic_b.wait_for_msg(None)
            self.msg_handler(msg)

    def msg_handler(self, msg):
        """ Handle received message """
        msg_size = struct.unpack('i', msg[0:4])
        msg_dict = json.loads(msg[4:5+msg_size[0]])

        # if "new-group" received
        if 'new_group' in msg_dict:
            # TODO cancel tasks
            self.send_present(msg_dict['gid'])
            # TODO schedule mebership check at groupid + period
            pass

        elif 'present' in msg_dict:
            #TODO understand M
            if self.group is None and True:
                self.group = msg_dict['gid']
                self.members = True #TODO change to M
            pass

        elif 'list' in msg_dict:
            if True: #TODO check time < O and gamma
                self.last_list = O #TODO this is O not 0
                if not host.id == max(self.members):
                    #TODO forward list
                    pass
            pass

    def send_reconfigure(self):
        """ Sends a reconfigure request for the group consisting of the id """
        t = time.time()
        msg_dict = {'new_group' : True,
                    'gid' : t }
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        self.atomic_b.broadcast(msg_size + msg_bytes)

    def send_present(self, t):
        msg_dict = {'present' : True,
                    'gid' : t,
                    'id' : self.host.id }
        msg_bytes = json.dumps(msg_dict).encode()
        msg_size = struct.pack('i', len(msg_bytes))
        self.atomic_b.broadcast(msg_size + msg_bytes)

    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.members


    def __membership_confirmation_task(self, check_time):
        LOG.info("%i: members arrived before check %s",
                 self.host.id, self.check_members)
        if self.check_members != self.cur_members:
            LOG.debug("confirming: UPDATING mems from %s to %s",
                      self.cur_members, self.check_members)
            self.cur_members = self.check_members
            self.cur_group += self.period
            # reset
            self.check_members = {self.host.id}
        else:
            self.cur_group += self.period
            LOG.debug("%i: confirming: OKAY mems at %s", self.host.id, self.cur_members)
            self.check_members = {self.host.id}

        # schedule next check task; next check at check_time + period
        next_check_time = check_time + self.period
        next_check_task = th.Timer(next_check_time - time.time() - 1,
                                   self.__membership_check_task,
                                   args=(next_check_time,))
        self.scheduled_broadcasts[next_check_time] = next_check_task
        next_check_task.start()


    def __membership_check_task(self, check_time):
        self.send_present()
        confirm_time = check_time + self.delta
        confirm_task = th.Timer(confirm_time - time.time(),
                                self.__membership_confirmation_task,
                                args=(check_time,))
        confirm_task.start()

