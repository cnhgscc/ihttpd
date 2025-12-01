from ._ihttpd import multi_read, push_read, wait_read


__all__ = ["multi_read", "push_read", "wait_read"]


def multi_download(*args, **kwargs):
    multi_read(*args, **kwargs)


def push(name: str):
    push_read(name)


def wait():
    wait_read()
