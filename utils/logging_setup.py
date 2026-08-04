import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging() -> None:
    """Loga em stdout, não stderr -- terminais (PyCharm incluso) pintam
    stderr de vermelho por padrão, o que faz até uma linha INFO comum
    parecer um erro. Só exceções não tratadas (traceback do Python) ainda
    vão para stderr, o que é o comportamento certo pra esse caso."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
