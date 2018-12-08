import multiprocessing as mp
from channel import Channel, Message


class AtomicBroadcaster(object):

    def __init__(self, hosts, ports):
        self.msg_queue = mp.Queue()
        self.channels = [Channel(hosts, port, self.msg_queue) for port in ports]
        self.__forwarder = mp.Process(target=self.__forwarder_worker,
                                      daemon=True)
        self.__forwarder.start()

    #TODO currently exerimental code for select could just use pipes if
    # necessary
    # currently select works because queue uses a pipe underlying,
    # but the select only returns a _reader object thus maybe do a
    # _reader dictionary to queue dictionary or do a filer but that might be
    # slow
    #
    # could also just use 1 queue and block that way but i think its a little
    # ugly to pass in a queue into a constructor but probably best option
    #def __forwarder_worker(self):
    #    """ Wait on multiple Queue objects and forward if timely """
    #    queues = [q._reader for q in c.queue for c in self.channels]
    #    (input,[],[]) = select.select([que._reader],[],[])
    #    pass

    def __forwarder_worker(self):
        while True:
            self.msg_queue.get()

    # Send message on all channels
    def broadcast(self, message):
        for c in self.channels:
            c.broadcast(message)
