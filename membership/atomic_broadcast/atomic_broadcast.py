import functools
import time
import threading as th
import multiprocessing as mp
from membership.atomic_broadcast.channel import Channel, Message
from membership import LOG


class AtomicBroadcaster(object):

    def __init__(self, server_port, hosts, channel_count):
        """
        :server_port: port of the server that this AtomicBroadcaster is running
        on
        :hosts: dict of hosts running in the cluster
        :channel_count: number of channels to create
        """
        self.msg_queue = mp.Queue()
        self.hosts = hosts
        self.channel_count = channel_count
        self.server_port = server_port
        self.c_hist_lock = th.Lock()
        self.c_history = {}
        self.channels = [Channel(server_port, n + 1, self.msg_queue)
                         for n in range(channel_count)]
        # LOG.info("hosts: %s", self.hosts)
        # TODO this is not accurate, need to flushout calc_sigma
        self.sigma = 1
        self.message_list = MessageList()
        self.__forwarder = th.Thread(target=self.__forwarder_worker)
        self.__forwarder.start()

        # XXX
        # if server_port == 50000:
            # m = Message(b'123451234512345', b'HI', 0)

            # time.sleep(2)
            # self.broadcast(m)

    def __add_to_c_history(self, msg):
        """
        :return: True if msg is new else False
        """
        self.c_hist_lock.acquire()
        new = False
        key = (msg.time, msg.addr)
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
            if not msg.is_late(self.channel_count // 2, self.sigma):
                if msg.is_timely(self.sigma):
                    # self.message_list.add_message(accept_time, msg)
                    if self.__add_to_c_history(msg):
                        # schedule forwarding task
                        self.__schedule_forward_task(msg)
                else:
                    LOG.debug('not timely')
            else:
                LOG.debug('is late')
                # k = self.channel_count // 2
                # LOG.debug("is_late: %f < (%f + (%i + 1) * %i)",
                # msg.recv_time, msg.time, k, self.sigma)
                # LOG.debug("\n%f\n%f", msg.recv_time, msg.time + (k+1)*self.sigma)


    def __schedule_forward_task(self, msg):
        # LOG.debug("scheduling forwarding task")
        forward_task = functools.partial(self.__forward_task, msg=msg)
        task_thread = th.Thread(target=forward_task)
        task_thread.daemon = True
        task_thread.start()

    def __forward_task(self, msg):
        """ determine the channels to forward msg on """
        duration = msg.get_timely_deadline(self.sigma) - time.time()
        # LOG.debug('sleeping for %f in task', duration)
        time.sleep(duration)

        self.c_hist_lock.acquire()

        highest_chan_recv = self.c_history[(msg.time, msg.addr)]['c']  # c
        highest_chan_send = self.channel_count - msg.hops  # f + 1 - h

        # check if c < f + 1 - h
        if highest_chan_recv < highest_chan_send:
            LOG.info("%i is forwarding msg from %i", self.server_port,
                    msg.addr[1])
            msg.add_hop()
            # forward on channels c + 1, ..., f + 1 - h
            for channel in self.channels[highest_chan_recv:highest_chan_send]:
                for _, host in self.hosts.items():
                    channel.send(host.ip, host.port, msg)

        self.c_hist_lock.release()

    def calc_sigma(self):
        """ Find average ping to all hosts """
        # TODO we can use popen to invoke ping but that may be poor style
        pass

    def get_messages(self):
        return self.message_list.get_messages()

    # Send message on all channels
#<<<<<<< HEAD
#    def broadcast(self, message):
#        message_out = None
#        for c in self.channels:
#            #TODO need to get host ip for first argument
#            message_out = Message(None, message, c.channel_id)
#            for _, host in self.hosts.items():
#                c.send(host.ip, host.port, message_out)
#=======
    def broadcast(self, msg):
        msg.time = time.time()
        for channel in self.channels:
            for _, host in self.hosts.items():
                channel.send(host.ip, host.port, msg)

#>>>>>>> b00f0a934aa847ca54717142faed4367cf0f1fe5


class MessageList(object):
    # intermal format should be (time to accept message, message)
    # output list should just be message objects

    def __init__(self):
        self.messages = list()
        self.lock = th.Lock()
        self.sema = th.Semaphore(value=0)

    def get_messages(self):
        t = time.time()
        out = list()
        last_index = 0
        self.lock.aqcuire()
        t = time.time()
        for i, message in enumerate(self.messages):
            # if message is ready to be received
            if message[0] > t:
                out.append(message[1])
                last_index = i+1
            else:
                break
        self.messages = self.messages[last_index:]
        self.lock.release()
        return out

    def add_message(self, accept_time, new_message):
        self.lock.aqcuire()
        for i, message in enumerate(self.messages):
            if accept_time < message.time[0]:
                self.messages.insert(i, (accept_time, new_message))
                # TODO experiemental
                if i == 0:
                    self.sema.release() # signal that there is a new first item
        self.lock.release()

    def wait_for_msg(self, timeout):
        if self.sema.acquire(True, timeout):
            return self.peek()
        else:
            return None


    def peek(self):
        """ Gives the first item of the MessageList """
        ret = None
        self.lock.aqcuire()
        if len(self.messages) > 0:
            ret = self.messages[0]
        self.lock.release()
        return ret
