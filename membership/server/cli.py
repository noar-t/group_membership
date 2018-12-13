import argparse

NAME = 'server'
DESC = 'server commands'

ARG_SETS = {
    'LOG': (
        (('-v', '--verbose'),
         {'help': 'enable debug messages', 'action': 'store_true',
          'default': False}),),
    'SERVER': (
        (('-i', '--id'),
         {'help': 'globally unique id for the server', 'required': True,
          'metavar': 'INT', 'type': int, 'action': 'store'}),
        (('-s', '--servers'),
         {'help': 'list of servers in the cluster', 'metavar': 'IP:ID',
          'nargs': '*'}),),
    'BC': (
        (('-f', '--file'),
         {'help': 'file for failure configuration', 'required': True,
          'metavar': 'STR', 'type': str, 'action': 'store'}),
        (('-m', '--message'),
         {'help': 'message to broadcast', 'required': True,
          'metavar': 'STR', 'type': str, 'action': 'store'}),),
}
SUBCMDS = {
    'bc': ('run server program', ('BC',)),
    # 'cluster': ('run cluster program', ('LOG', 'COUNT')),
}


def configure_parser():
    """
    configure the argument parser
    :returns: argparse.ArgumentParser
    """
    # configure parser
    parser = argparse.ArgumentParser(description=DESC, prog=NAME)
    subparsers = parser.add_subparsers(dest="subcmd")
    subparsers.required = True

    def add_subparser(name, desc, arg_sets):
        subparser = subparsers.add_parser(name, help=desc)
        for (_args, _kwargs) in (a for arg_set in arg_sets for a in arg_set):
            subparser.add_argument(*_args, **_kwargs)

    # configure subparsers
    for (name, (desc, arg_sets)) in SUBCMDS.items():
        add_subparser(name, desc, [ARG_SETS[a] for a in arg_sets])

    return parser
