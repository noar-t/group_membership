import functools
import time
import threading as th
import multiprocessing as mp
from membership.atomic_broadcast.channel import Channel, Message
from membership import LOG


class AtomicBroadcaster(object):

    def __init__(self, server_id, server_port, hosts, channel_count):
        """
        :server_port: port of the server that this AtomicBroadcaster is running
        on
        :hosts: dict of hosts running in the cluster
        :channel_count: number of channels to create
        """
        self.server_id = server_id
        self.msg_queue = mp.Queue()
        self.hosts = hosts
        self.config = None
        self.channel_count = channel_count
        self.server_port = server_port
        self.c_hist_lock = th.Lock()
        self.c_history = {}
        self.channels = [Channel(server_port, n + 1, self.msg_queue)
                         for n in range(channel_count)]
        self.sigma = 1
        self.delivery_delay = ((self.channel_count // 2) + 1) * self.sigma
        self.message_list = MessageList()
        self.__forwarder = th.Thread(target=self.__forwarder_worker)
        self.__forwarder.start()

    def __add_to_c_history(self, msg):
        """
        :return: True if msg is new else False
        """
        self.c_hist_lock.acquire()
        new = False
        key = (msg.time, msg.host)
        if key not in self.c_history:
            self.c_history[key] = {'msg': msg, 'c': msg.chan}
            new = True
        elif self.c_history[key]['c'] < msg.chan:
            self.c_history[key]['c'] = msg.chan
        self.c_hist_lock.release()

        return new

    # Forwards message (T,s,,h+1), on channels c+1,...,f+1-h
    def __forwarder_worker(self):
        while True:
            msg = self.msg_queue.get()
            # LOG.info("forward_worker msg hops:%i", msg.hops)
            # if the msg came from outself, don't forward and don't add to
            # msglist
            if msg.host == self.server_id:
                continue
            if msg.hops == -1:
                self.message_list.add_message(0, msg)
                continue
            if not msg.is_late(self.channel_count // 2, self.sigma):
                if msg.is_timely(self.sigma):
                    if self.__add_to_c_history(msg):
                        # schedule forwarding task
                        delivery_time = msg.get_delivery_time(
                            self.channel_count // 2,
                            self.sigma)
                        LOG.debug("adding msg at %i", self.server_id)
                        self.message_list.add_message(delivery_time, msg)
                        self.__schedule_forward_task(msg)
            #    else:
            #        LOG.debug('not timely')
            # else:
            #    LOG.debug('is late')
                # k = self.channel_count // 2
                # LOG.debug("is_late: %f < (%f + (%i + 1) * %i)",
                # msg.recv_time, msg.time, k, self.sigma)
                # LOG.debug("\n%f\n%f", msg.recv_time, msg.time + (k+1)*self.sigma)

    def __schedule_forward_task(self, msg):
        # LOG.debug("scheduling forwarding task")
        forward_task = functools.partial(self.__forward_task, msg=msg)
        task_thread = th.Thread(target=forward_task)
        task_thread.start()

    def __forward_task(self, msg):
        """ determine the channels to forward msg on """
        duration = msg.get_timely_deadline(self.sigma) - time.time()
        # LOG.debug('sleeping for %f in task', duration)
        time.sleep(duration)

        self.c_hist_lock.acquire()

        highest_chan_recv = self.c_history[(msg.time, msg.host)]['c']  # c
        highest_chan_send = self.channel_count - msg.hops  # f + 1 - h

        # check if c < f + 1 - h
        if highest_chan_recv < highest_chan_send:
            LOG.info("%i is forwarding msg from %i", self.server_port,
                     msg.host)
            msg.add_hop()
            # forward on channels c + 1, ..., f + 1 - h
            for channel in self.channels[highest_chan_recv:highest_chan_send]:
                for _, host in self.hosts.items():
                    if host.id == self.server_id:
                        continue
                    channel.send(host.ip, host.port, msg)

        self.c_hist_lock.release()

    def broadcast(self, msg_data):
        """Send message on all channels"""
        msg = Message(None, msg_data, None)
        msg.time = time.time()
        msg.host = self.server_id
        for channel in self.channels:
            for _, host in self.hosts.items():
                if host.id == self.server_id:
                    continue
                channel.send(host.ip, host.port, msg)

    def wait_for_msg(self, timeout):
        """ Block until timout or a message arrives """
        return self.message_list.get_message(timeout, True)

    def configure(self, config):
        self.config = config

    def test_broadcast(self, msg_data, config=None):
        if config is None:
            config = self.config
        if config is None:
            return self.broadcast(msg_data)

        msg = Message(None, msg_data, None)
        msg.time = time.time()
        msg.host = self.server_id

        server_config = config[str(self.server_id)]
        for c in self.channels:
            channel_config = server_config[str(c.channel_id)]
            if channel_config[0][0] == 0:
                LOG.info("skipping c%i at server %i", c.channel_id,
                         self.server_id)
                continue
            delay = channel_config[0][1]
            if delay > 0:
                LOG.info("add %i delay to c%i at server %i", delay,
                         c.channel_id, self.server_id)
                time.sleep(delay)
            for id, host in self.hosts.items():
                if id == self.server_id:
                    continue
                if channel_config[id + 1] == 0:
                    # continue
                    break
                c.send(host.ip, host.port, msg)

    def destroy(self):
        for chan in self.channels:
            chan.destroy()
        LOG.info("destroying broadcaster")


class MessageList(object):
    """ Message queue to deliver messages at the correct time """

    def __init__(self):
        self.messages = mp.Queue()

    def get_message(self, timeout, blocking=False):
        try:
            return self.messages.get(blocking, timeout)
        except Exception:  # should be queue.Empty but not sure namespace
            return None

    def add_message(self, accept_time, msg):
        """ Put a message in the queue at accept_time """
        # LOG.info("m%s added for time %i", msg, accept_time)
        th.Thread(target=self.__add_message, args=(accept_time, msg)).start()

    def __add_message(self, accept_time, msg):
        """ Helper for add message, waits until accept time and enqueues msg """
        wait_t = accept_time - time.time()
        if wait_t > 0:
            time.sleep(wait_t)
        self.messages.put(msg)
