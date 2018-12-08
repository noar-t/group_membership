import logging


def _configure_log():
    """
    configure logging
    :returns: logger instance
    """
    log = logging.getLogger(__name__)
    formatter = logging.Formatter("%(levelname)s: %(asctime)s: %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    log.addHandler(console)
    return log


LOG = _configure_log()
