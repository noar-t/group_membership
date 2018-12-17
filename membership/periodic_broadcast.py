import struct
import time
import threading as th
from membership import LOG


class PeriodicBroadcastGroup(object):

    msg_fmt = '?di'  # new-group(t/f), groupid, id

    def __init__(self, broadcaster, host, join, period=10):
        self.host = host
        self.atomic_b = broadcaster
        self.period = period

        self.delta = broadcaster.delivery_delay
        self.cur_members = set()
        self.check_members = set()

        self.cur_group = None
        self.cur_period = 0
        self.scheduled_broadcasts = {}

        self.__b_thread = th.Thread(target=self.__broadcast_worker)
        self.__b_thread.start()

        time.sleep(2)
        if join:
            # self.atomic_b.delivery_delay += 1
            new_group_time = time.time() + self.delta
            self.cur_group = new_group_time
            self.send_broadcast(new_group_time, new_group=True)
            self.check_members.add(self.host.id)

            # scheulde a confirm for the new group. and then schedule a new check
            # for next period
            confirm_time = new_group_time + 2 * self.delta
            confirm_task = th.Timer(confirm_time - time.time(),
                                    self.__membership_confirmation_task,
                                    args=(new_group_time,))
            self.scheduled_broadcasts[confirm_time] = confirm_task
            confirm_task.start()

    def get_members(self):
        """ Returns a list of the most recent members of the group """
        return self.cur_members

    def __broadcast_worker(self):
        """ Broadcasts present every period time units """
        # group should be V + pi because of reconfiguration latency
        # create new group upon initialization
        # self.atomic_b.delivery_delay += 1
        # new_group_time = time.time() + self.atomic_b.delivery_delay
        # self.cur_group = new_group_time
        # self.send_broadcast(new_group_time, new_group=True)
        # self.check_members.add(self.host.id)

        # # scheulde a confirm for the new group. and then schedule a new check
        # # for next period
        # confirm_task = th.Timer(self.atomic_b.delivery_delay,
                                # self.__membership_confirmation_task,
                                # args=(new_group_time,))
        # confirm_task.start()




        # check_time = new_group_time + self.period
        # check_task = th.Timer(check_time, self.__membership_check_task,
                              # args=(check_time,))
        # self.scheduled_broadcasts[check_time] = check_task
        # check_task.start()

        # Processs messages and broadcast present
        while True:
            msg = self.atomic_b.wait_for_msg(None)
            self.msg_handler(msg)

    def send_broadcast(self, time, new_group=False):
        """ Broadcast a message to all hosts """
        if new_group:
            LOG.debug("Host:%i, Sending new_group:%f", self.host.id, time)
        else:
            LOG.debug("Host:%i, Sending present gid:%f", self.host.id, time)

        msg = struct.pack(self.msg_fmt, new_group,
                          time, self.host.id)
        self.atomic_b.broadcast(msg)

    def __membership_confirmation_task(self, check_time):
        LOG.info("\033[95 m%i: members arrived before check %s\033[0m",
                 self.host.id, self.check_members)
        if self.check_members != self.cur_members:
            LOG.debug("confirming: UPDATING mems from %s to %s",
                      self.cur_members, self.check_members)
            self.cur_members = self.check_members
            self.cur_group += self.period
            # reset
            self.check_members = set([self.host.id])
        else:
            # XXX
            self.cur_group += self.period
            LOG.debug("%i: confirming: OKAY mems at %s", self.host.id, self.cur_members)
            self.check_members = set([self.host.id])

        # schedule next check task; next check at check_time + period
        next_check_time = check_time + self.period
        next_check_task = th.Timer(next_check_time - time.time() - 1,
                                   self.__membership_check_task,
                                   args=(next_check_time,))
        self.scheduled_broadcasts[next_check_time] = next_check_task
        next_check_task.start()


    def __membership_check_task(self, check_time):
        self.send_broadcast(self.cur_group)
        confirm_time = check_time + self.delta
        confirm_task = th.Timer(confirm_time - time.time(),
                                self.__membership_confirmation_task,
                                args=(check_time,))
        self.scheduled_broadcasts[confirm_time] = confirm_task
        confirm_task.start()

    def msg_handler(self, msg):
        """ Handle receipt of broadcasts """
        msg = struct.unpack(self.msg_fmt, msg.data[:20])
        # if on time; myclock > V abort
        # if msg[1] < time.time():
        # if msg[1] > time.time():
        if True:
            # if "new-group" received
            if msg[0]:
                # cancel broadcasts
                # LOG.debug("new group %s", self.scheduled_broadcasts)
                for key, task in self.scheduled_broadcasts.items():
                    # if msg[0] <= key:
                    task.cancel()
                self.scheduled_broadcasts = {}

                LOG.info("%i: new group requested", self.host.id)
                self.cur_group = msg[1]
                self.cur_period = 0
                self.check_members = set([self.host.id, msg[2]])
                self.send_broadcast(msg[1])

                # schedule check for the new group req
                confirm_task = th.Timer(2 * self.delta,
                                        self.__membership_confirmation_task,
                                        args=(msg[1],))

                confirm_task.start()

            # if "present" received
            else:
                LOG.info("%i: member added %d", self.host.id, msg[2])
                # put the member in the group
                self.check_members.add(msg[2])
                # LOG.info("%i: members after add %s", self.host.id,
                         # self.check_members)
        else:
            LOG.info("LATE %s, %s", msg[1], time.time())
