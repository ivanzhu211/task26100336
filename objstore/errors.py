"""Exception types raised by the object store."""


class StoreError(Exception):
    """Base class for all object-store errors."""


class QuotaExceeded(StoreError):
    """Raised when an upload would exceed the account quota."""


class AccountNotFound(StoreError):
    """Raised when the account does not exist."""


class ObjectNotFound(StoreError):
    """Raised when the object does not exist (or is not in the expected state)."""


class SessionNotFound(StoreError):
    """Raised when the multipart session does not exist."""


class SessionClosed(StoreError):
    """Raised when operating on an aborted/expired multipart session."""


class SizeMismatch(StoreError):
    """Raised when actual data size differs from the declared size."""


class ObjectTooLarge(StoreError):
    """Raised when the declared size exceeds the maximum object size."""


class InvalidPart(StoreError):
    """Raised when a part number or part size is invalid."""


class CommitIncomplete(StoreError):
    """Raised when committing a session with missing parts or wrong total size."""
