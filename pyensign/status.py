class ServerStatus(object):
    def __init__(self, status, version, uptime):
        # TODO: convert status enum integer into string
        # TODO: convert uptime into a timedelta
        # TODO: handle the other fields returned from client.status
        self.status = status
        self.version = version
        self.uptime = uptime

    def __str__(self):
        return f"status: {self.status}\nversion: {self.version}\nuptime: {self.uptime}"
