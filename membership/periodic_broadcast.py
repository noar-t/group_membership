import struct
import time
import threading as th
from membership import LOG


class PeriodicBroadcastGroup(object):

    msg_fmt = '?di'  # new-group(t/f), groupid, id

    def __init__(self, broadcaster, host, period=10):
        self.past_members = set()
        self.cur_members = set()

        self.cur_group = None
        self.cur_period = 0
        self.host = host
        self.period = period
        self.atomic_b = broadcaster
        self.scheduled_broadcasts = {}

        self.__b_thread = th.Thread(target=self.__broadcast_worker)
        self.__b_thread.start()

    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.past_members

    def __broadcast_worker(self):
        """ Broadcasts present every period time units """
        # group should be V + pi because of reconfiguration latency
        # create new group upon initialization
        self.cur_group = time.time() + self.atomic_b.delivery_delay
        self.send_broadcast(new_group=True)
        self.send_broadcast()
        # self.__broadcast_task(time.time())
        self.__schedule_broadcast_task(time.time() + self.period)

        # Processs messages and broadcast present
        while True:
            # timeout = self.period - ((time.time() - self.cur_group) % self.period)
            # LOG.info("waiting timeout %f", timeout)
            msg = self.atomic_b.wait_for_msg(None)
            # if there were no messages, the period is over
            # if msg is None:
                # LOG.info("\033[95 mmembers at end of period %s\033[0m", self.get_members())
                # self.past_members = self.cur_members
                # self.cur_members = set([self.host.id])
                # self.cur_period += 1
                # self.send_broadcast()
            # else:
                # self.msg_handler(msg)
            self.msg_handler(msg)

    def __schedule_broadcast_task(self, broadcast_time):
        # broadcast_task = functools.partial(self.__broadcast_task, broadcast_time)
        # task_thread = th.Thread(target=broadcast_task)
        # self.scheduled_broadcasts[broadcast_time] = True
        # task_thread.daemon = True
        # task_thread.start()

        broadcast_task = th.Timer(broadcast_time - time.time(),
                                  self.__broadcast_task,
                                  args=(broadcast_time,))
        self.scheduled_broadcasts[broadcast_time] = broadcast_task
        broadcast_task.start()

    def __broadcast_task(self, broadcast_time):
        # wait_duration = broadcast_time() - time.time()
        # time.sleep(wait_duration)
        # if self.scheduled_broadcasts[broadcast_time]:
        LOG.info("\033[95 mmembers at end of period %s\033[0m", self.get_members())
        self.past_members = self.cur_members
        self.cur_members = set([self.host.id])
        self.cur_period += 1
        self.send_broadcast()
        self.__schedule_broadcast_task(broadcast_time + self.period)
        del self.scheduled_broadcasts[broadcast_time]

    def send_broadcast(self, new_group=False):
        """ Broadcast a message to all hosts """
        if new_group:
            LOG.debug("Host:%i, Sending new_group:%f", self.host.id, self.cur_group)
        else:
            LOG.debug("Host:%i, Sending present gid:%f", self.host.id, self.cur_group)

        msg = struct.pack(self.msg_fmt, new_group,
                          self.cur_group, self.host.id)
        self.atomic_b.broadcast(msg)

    def msg_handler(self, msg):
        """ Handle receipt of broadcasts """
        msg = struct.unpack(self.msg_fmt, msg.data[:20])
        # if on time; myclock > V abort
        if msg[1] < time.time():
            # if new-group
            if msg[0]:
                LOG.info("new group requested")
                self.cur_group = msg[1]
                self.cur_period = 0
                self.cur_members = set([self.host.id])
                self.past_members = set()

                # cancel broadcasts
                for key, task in self.scheduled_broadcasts.items():
                    task.cancel()
                self.scheduled_broadcasts = {}
                self.__schedule_broadcast_task(msg[1] + self.period)

            # present broadcast
            else:
                LOG.info("member added %d", msg[2])
                # put the member in the group
                self.cur_members.add(msg[2])
