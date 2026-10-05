"""Exception hierarchy. Retry logic keys off these types, so they are deliberate."""


class IngestionError(Exception):
    """Base class for every ingestion failure."""


class RetryableError(IngestionError):
    """A transient failure (timeout, 429, 5xx) that is worth retrying."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class SourceAuthError(IngestionError):
    """Credentials were rejected. Retrying cannot help, so this is never retried."""


class InvalidResponseError(IngestionError):
    """The source answered, but not with the contract we expect."""


class InvalidSourceFile(IngestionError):
    """A source file is structurally broken (bad JSON, missing columns, ragged CSV)."""


class ConfigurationError(IngestionError):
    """Pipeline configuration is missing or inconsistent."""
