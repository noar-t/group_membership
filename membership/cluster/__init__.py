import os
from subprocess import Popen
from membership import LOG


class Cluster(object):
    """main cluster object"""

    def __init__(self, args):
        self.servers = {}
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

            self.servers[id] = {'process': p, 'log': log}

    def add(self, id):
        """
        adds server id to the cluster
        :id: new server's id
        """
        if id in self.servers:
            LOG.error("server %i already exists", id)
            return False
        elif id < 0 or id > 100:
            LOG.error("id %i is out of range [0, 100]", id)
            return False

        # TODO clean up
        args = ['python3', '-m', 'membership', 'server', '-c',
                str(self.args.count)]

        if self.args.verbose:
            args.append('-v')

        id_arg = ['-i', str(id)]
        # open log file
        log = open(os.path.join('logs', str(id) + '.log'), 'w')
        LOG.info('starting server: %i', id)
        # spawn child process
        p = Popen(args + id_arg, stdout=log, stderr=log)
        self.servers[id] = {'process': p, 'log': log}
        return True

    def remove(self, id):
        """
        removes server id from the cluster
        :id: server id to remove
        """
        if id not in self.servers:
            LOG.error("server %i does not exists", id)
            return False
        LOG.info("forcibly termination server %i", id)
        self.servers[id]['process'].kill()
        del self.servers[id]

    def kill(self):
        """terminate cluster"""

        for _, server in self.servers.items():
            server['process'].kill()
            server['log'].close()

        LOG.info('cluster killed')

    def __str__(self):
        return '\n'.join([str(id) for id in self.servers])
