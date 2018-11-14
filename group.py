
class(object):
    def __init__(self, leader):
        self.leader = leader
        self.members = list()

    @property
    def leader(self):
        return self.leader

    @property
    def members(self):
        return self.members

    def add_member(self, member):
        self.members.append(member)
