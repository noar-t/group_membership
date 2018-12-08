import os
from subprocess import Popen
from membership import LOG


class Cluster(object):
    """main cluster object"""

    def __init__(self, args):
        self.servers = []
        self.args = args

    def start(self):
        """start cluster"""
        # NOTE change this to support running across machines

        args = ['python3', '-m', 'membership', 'server', '-c',
                str(self.args.count)]

        if self.args.verbose:
            args.append('-v')

        LOG.info('log files for servers can be found at logs/')

        for id in range(self.args.count):
            id_arg = ['-i', str(id)]

            LOG.info('starting server: %i', id)

            # open log file
            log = open(os.path.join('logs', str(id) + '.log'), 'w')

            # spawn child process
            p = Popen(args + id_arg, stdout=log, stderr=log)

            self.servers.append({'process': p, 'log': log})

        # TODO add cli support
        try:
            # wait on servers
            for server in self.servers:
                server['process'].wait()
        except KeyboardInterrupt:
            self.kill()

    def kill(self):
        """terminate cluster"""

        for server in self.servers:
            server['process'].kill()
            server['log'].close()

        LOG.info('cluster killed')
