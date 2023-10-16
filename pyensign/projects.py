from pyensign.utils import ulid
from pyensign.topics import Topic


class Project(object):
    """
    A project is a collection of topics. Similar to a traditional database, if you have
    access to the project, you have access to all of its topics.
    """

    def __init__(self, id):
        self.id = ulid.parse(id)
        self.num_topics = 0
        self.num_readonly_topics = 0
        self.events = 0
        self.duplicates = 0
        self.data_size_bytes = 0
        self.topics = []

    def __repr__(self):
        return "Project(id={})".format(self.id)

    def __str__(self):
        s = "Project"
        s += "\n\tid={}".format(self.id)
        s += "\n\tnum_topics={}".format(self.num_topics)
        s += "\n\tnum_readonly_topics={}".format(self.num_readonly_topics)
        s += "\n\tevents={}".format(self.events)
        s += "\n\tduplicates={}".format(self.duplicates)
        s += "\n\tdata_size_bytes={}".format(self.data_size_bytes)
        for topic in self.topics:
            s += "\n\t"
            s += str(topic).replace("\t", "\t\t")
        return s

    @classmethod
    def from_info(cls, pb_val):
        """
        Convert a protocol buffer ProjectInfo into a Project.
        """

        project = cls(pb_val.project_id)
        project.num_topics = pb_val.num_topics
        project.num_readonly_topics = pb_val.num_readonly_topics
        project.events = pb_val.events
        project.duplicates = pb_val.duplicates
        project.data_size_bytes = pb_val.data_size_bytes
        for topic in pb_val.topics:
            project.topics.append(Topic.from_info(topic))
        return project
