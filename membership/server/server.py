import json
import socket
import selectors
import types
from membership import LOG
from membership.atomic_broadcast.atomic_broadcast import AtomicBroadcaster
from membership.server import cli
from membership.periodic_broadcast import PeriodicBroadcastGroup
from membership.attendance_list import AttendanceListGroup


class Host(object):
    def __init__(self, id, ip):
        self.id = id
        self.ip = ip
        self.port = 50000 + 100 * id


class Server(object):
    def __init__(self, args):
        self.port = 50000 + 100 * args.id
        self.servers = {}
        LOG.debug("starting server %i with port %i", args.id, self.port)
        LOG.debug("starting server with args %s", args)

        for server in args.servers:
            split = server.split(':')
            id = int(split[1])
            ip = split[0]
            self.servers[id] = Host(id, ip)
        self.servers[args.id] = Host(args.id, split[0])

        # LOG.debug("server list %s", self.servers)

        self.broadcaster = AtomicBroadcaster(args.id, self.port,
                                             self.servers, 1)

        # check if we running an membership protocol
        if args.protocol == 'periodic':
            self.periodic_group = PeriodicBroadcastGroup(self.broadcaster,
                                                         self.servers[args.id],
                                                         args.join)
        elif args.protocol == 'list':
            self.attendance = AttendanceListGroup(self.broadcaster,
                                                  self.servers[args.id])
        elif args.protocol == 'neighbor':
            pass

        self.commands = {
            'bc': self.__test_broadcast,
            'config': self.__configure_channels,
            'destroy': self.__destroy,
        }

        self.parser = cli.configure_parser()
        while True:
            cmd = input()
            LOG.info("cmd: %s", cmd)
            cmd = cmd.split()

            args = self.parser.parse_args(cmd)
            LOG.info(args)
            if args.subcmd not in self.commands:
                LOG.info("cmd not found")

            self.commands[args.subcmd](args)

    def __test_broadcast(self, args):
        config = None
        if args.file is not None:
            with open(args.file) as f:
                config = f.read()
                config = json.loads(config)
                LOG.debug("json loaded %s", config)

        # self.broadcaster.broadcast(' '.join(args.message).encode())
        # self.__configure_channels(self, args)
        self.broadcaster.test_broadcast(' '.join(args.message).encode(),
                                        config)

    def __configure_channels(self, args):
        if args.file is not None:
            with open(args.file) as f:
                config = f.read()
                config = json.loads(config)
                LOG.debug("json loaded %s", config)
        self.broadcaster.configure(config)

    def __destroy(self, args):
        self.broadcaster.destroy()

    def start(self):
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
