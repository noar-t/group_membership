import sys
import logging
from membership import LOG, cli, cluster
from membership.server import server


def run_server(args):
    s = server.Server(args)
    s.start()


def run_cluster(args):
    c = cluster.Cluster(args)
    c.start()


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
