# Import Python's built-in logging framework.
import logging


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a configured logger.

    Parameters
    ----------
    name:
        Name of the module or pipeline using the logger.

    Returns
    -------
    logging.Logger
        Configured Python logger.
    """

    # Get or create a logger with the supplied name.
    logger = logging.getLogger(name)

    # Set the minimum logging level.
    # INFO means:
    # DEBUG messages are ignored,
    # while INFO, WARNING, ERROR and CRITICAL are recorded.
    logger.setLevel(logging.INFO)

    # Avoid adding another handler every time this function
    # is called in the same Python process.
    if not logger.handlers:

        # Send log messages to the console.
        handler = logging.StreamHandler()

        # Define the structure of each log message.
        formatter = logging.Formatter(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        )

        # Attach the formatter to the console handler.
        handler.setFormatter(formatter)

        # Attach the handler to the logger.
        logger.addHandler(handler)

    return logger