class CommandError(Exception):
    pass


class Disconnect(Exception):
    pass


class Error(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message
