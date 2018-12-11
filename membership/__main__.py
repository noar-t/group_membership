import sys
import logging
from membership import LOG, cli, cluster
from membership.server import server


def add_server(c, *args):
    """
    :c: cluster object
    :id: id of server to add
    """
    # FIXME
    id = int(args[0])
    c.add(id)


def remove_server(c, *args):
    """
    :c: cluster object
    :id: id of server to remove
    """
    # FIXME
    id = int(args[0])
    LOG.info("removing server %i", id)
    c.remove(id)


def list_servers(c, *args):
    """
    lists running servers
    """
    print("Servers ids:")
    print(c)


OPTIONS = {
    'add': add_server,
    'remove': remove_server,
    'ls': list_servers,
}


def run_server(args):
    s = server.Server(args)
    s.start()


def run_cluster(args):
    def handle_cmd(c, cmd, *args):
        try:
            OPTIONS[cmd](c, *args)
        except KeyError:
            print("Command not found.")

    c = cluster.Cluster(args)
    c.start()
    try:
        while True:
            cmd = input()
            cmd = cmd.split()
            handle_cmd(c, cmd[0], *cmd[1:])
        # for server in self.servers:
            # server['process'].wait()
    except KeyboardInterrupt:
        c.kill()


def main():
    """main entry point"""
    command_handlers = {
        'server': run_server,
        'cluster': run_cluster,
    }

    parser = cli.configure_parser()
    args = parser.parse_args()

    LOG.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    command_handlers[args.subcmd](args)


if __name__ == "__main__":
    sys.exit(main())
