import logging

import pytest

from edp.common.logging import _HANDLER_MARKER, setup_logging


def test_setup_is_idempotent():
    setup_logging("INFO")
    setup_logging("DEBUG")
    handlers = [h for h in logging.getLogger().handlers if getattr(h, _HANDLER_MARKER, False)]
    assert len(handlers) == 1
    assert logging.getLogger().level == logging.DEBUG


def test_invalid_level_raises():
    with pytest.raises(ValueError):
        setup_logging("LOUD")
