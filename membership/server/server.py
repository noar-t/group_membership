import json
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
                                                  self.servers[args.id],
                                                  args.join)
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
