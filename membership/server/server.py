import argparse
import socket
import selectors
import types
import pickle
import time
from membership import LOG


class Server(object):

    def __init__(self, args):
        self.port = 50000 + 100 * args.id
        LOG.debug("starting server %i with port %i", args.id, self.port)

        self.event_loop(self.port)

    def event_loop(self, port):
        selector = selectors.DefaultSelector()

        # Need to listen for either a membership update or
        # a new group by TCP broadcast
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = socket.gethostname()
        server_sock.bind((host, port))
        server_sock.listen()
        print('listening on', (host, port))
        server_sock.setblocking(False)

        selector.register(server_sock, selectors.EVENT_READ, data=None)

        # Basic event loop
        while True:
            # TODO change timout to be variable
            events = selector.select(timeout=5)
            print('event triggered')

            if len(events) == 0:
                print('timeout')
                # TODO Broadcast to all hosts
            else:
                for key, mask in events:
                    if key.data is None:
                        accept_new_connection(selector, key.fileobj)
                    else:
                        service_connection(selector, key, mask)

    def service_connection(sel, key, mask):
        print('Key: ' + repr(dir(key.fileobj)))
        sock = key.fileobj
        data = key.data

        if mask & selectors.EVENT_READ:
            print('DEBUG EVENT_READ')
            # if data.request_type is None:
            # Todo recieve request type first
            recv_data = sock.recv(1024)  # Should be ready to read
            if recv_data:
                data.msg += recv_data
                print('recieved: ' + repr(recv_data))
            else:
                print('TODO closing connection to', data.addr)
                print('TODO recieved: ' + repr(data.msg))  # pickle.loads(data.msg)))
                sel.unregister(sock)
                sock.close()

        if mask & selectors.EVENT_WRITE:
            if data.request_type is None:
                data.request_type = data.msg
                data.msg = b''
                # time.sleep(10)
                sock.sendall(b'recieved')
                print('DEBUG EVENT_WRITE')
                print('DEBUG selector: ' + repr(sel))
                print('DEBUG key: ' + repr(key))

    def accept_new_connection(selector, sock):
        conn, addr = sock.accept()  # Should be ready to read
        conn.setblocking(False)
        print('accepted connection from', addr)

        data = types.SimpleNamespace(addr=addr, msg=b'', request_type=None)

        # TODO setup write for new group request
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        selector.register(conn, events, data=data)
