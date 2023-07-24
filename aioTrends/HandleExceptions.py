class Error(Exception):
    """Base class for other exceptions"""
    pass

class InputError(Error):
    """Raised when the input is wrong"""
    pass

class UnkownError(Error):
    """Raised unknown exceptions"""
    pass

class ProxyError(Error):
    """Raised errors related to proxy"""
    pass

class ResponseError(Error):
    """Raised response error"""
    pass

class QueryError(InputError):
    """Raised errors related to queries"""
    pass

