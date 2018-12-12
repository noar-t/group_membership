import multiprocessing as mp
import socket
import unittest

# from atomic_broadcast.broadcast.channel import Channel
from membership.atomic_broadcast.channel import Channel
# from membership.atomic_broadcast.atomic_broadcast import Host


class TestAtomicBroadcast(unittest.TestCase):
    """test channels"""


    def test_send_recv(self):
        """test channel send and receiving"""
        pass
        # localhost = socket.gethostbyname(socket.gethostname())

        # host1 = Host(localhost, 50001)
        # host2 = Host(localhost, 50002)

        # queue = mp.Queue()
        # ch1 = Channel(50001, queue)
        # ch2 = Channel(50002, queue)


        # ch1.send(host2, b'hi')
        # ch1.send(host2, b'hi2')
        
        # assert queue.get()[1] == b'hi'
        # assert queue.get()[1] == b'hi2'
