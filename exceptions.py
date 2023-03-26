__all__ = [
    'NoTokenError',
    'APIError',
    'AuthorizationTimeoutError',
    'AuthorizationCanceledError',
]


class NoTokenError(ValueError):
    pass


class APIError(ValueError):
    pass


class AuthorizationTimeoutError(TimeoutError):
    pass


class AuthorizationCanceledError(RuntimeError):
    pass
