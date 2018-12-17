import os
import sys
import time
from subprocess import Popen, PIPE
from membership import LOG


class Cluster(object):
    """main cluster object"""

    def __init__(self, args):
        self.servers = {}
        self.args = args

    def start(self):
        """start cluster"""
        for id in range(self.args.count):
            self.add(id, init=True)

    def add(self, id, ip='127.0.1.1', init=False, join=False):
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
        # if init:
            # args += [ip + ':' + str(id) for id in range(self.args.count)]
        args += [ip + ':' + str(i) for i in range(10)]

        if init and id == 0:
            args += ['-j']
        if join:
            args += ['-j']
        # else:
            # args += [ip + ':' + str(id) for id in self.servers]

        # LOG.debug("protocol: %s", self.args.protocol)
        if self.args.protocol == 'periodic':
            args += ['-p', self.args.protocol]
        elif self.args.protocol == 'neighbor':
            args += ['-p', self.args.protocol]
        elif self.args.protocol == 'list':
            args += ['-p', self.args.protocol]

        if self.args.verbose:
            args.append('-v')

        args += ['-i', str(id)]
        # open log file
        log = open(os.path.join('logs', str(id) + '.log'), 'w')
        LOG.info('starting server: %i', id)
        LOG.debug('arguments: %s', args)
        # spawn child process
        if self.args.debug:
            p = Popen(args, stdout=sys.stdout, stderr=sys.stderr, stdin=PIPE)
        else:
            p = Popen(args, stdout=log, stderr=log, stdin=PIPE)
        self.servers[id] = {'process': p, 'log': log}
        return True

    def send(self, id, buf):
        if id not in self.servers:
            LOG.error("server %i does not exists", id)
            return False
        LOG.debug("sending %s to server %i", buf, id)
        buf += '\n'
        self.servers[id]['process'].stdin.write(buf.encode())
        self.servers[id]['process'].stdin.flush()
        return True

    def remove(self, id):
        """
        removes server id from the cluster
        :id: server id to remove
        """
        if id not in self.servers:
            LOG.error("server %i does not exists", id)
            return False
        buf = 'destroy'
        LOG.debug("sending %s to server %i", buf, id)
        buf += '\n'
        self.servers[id]['process'].stdin.write(buf.encode())
        self.servers[id]['process'].stdin.flush()

        time.sleep(1)

        LOG.info("forcibly terminating server %i", id)
        self.servers[id]['log'].close()
        self.servers[id]['process'].kill()
        del self.servers[id]

    def kill(self):
        """terminate cluster"""

        for _, server in self.servers.items():
            server['log'].close()
            server['process'].kill()

        LOG.info('cluster killed')

    def __str__(self):
        return '\n'.join([str(id) for id in self.servers])
