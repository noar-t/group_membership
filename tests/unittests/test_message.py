import time
import unittest
from membership.atomic_broadcast.channel import Message


class TestMessage(unittest.TestCase):
    """test messages"""

    def test_marshal_unmarshal(self):

        for n in range(100):
            msg = Message(b'123451234512345', b'test12345', 4)
            msg.time = time.time()

            data = msg.marshal()
            msg2 = Message.from_binary_msg(data, None)


            assert msg.time == msg2.time
            assert msg.chan == msg2.chan
            assert msg.hops == msg2.hops
            # assert msg.data == msg2.data
