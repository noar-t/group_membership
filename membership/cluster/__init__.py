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
        for id in range(self.args.count):
            self.add(id)

    def add(self, id, ip='127.0.1.1'):
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
        args = ['python3', '-m', 'membership', 'server', '-s']
        # args += [ip + ':' + str(id) for id in self.servers]
        args += [ip + ':' + str(id) for id in range(self.args.count)]

        if self.args.verbose:
            args.append('-v')

        args += ['-i', str(id)]
        # open log file
        log = open(os.path.join('logs', str(id) + '.log'), 'w')
        LOG.info('starting server: %i', id)
        LOG.debug('arguments: %s', args)
        # spawn child process
        p = Popen(args, stdout=log, stderr=log)
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
