from enum import Enum

class RType(Enum):
    CHECK_IN = 0
    NEW_GROUP = 1


class Group(object):
    def __init__(self, members):
        self.members = members

    @property
    def members(self):
        return self.members

    def add_member(self, member):
        self.members.append(member)
