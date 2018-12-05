import multiprocessing as mp
import unittest

# from atomic_broadcast.broadcast.channel import Channel
from atomic_broadcast.channel import Channel


class TestChannel(unittest.TestCase):
    """test channels"""


    def test_send_recv(self):
        """test channel send and receiving"""

        queue = mp.Queue()
        ch = Channel(["localhost"], 50001, queue)

