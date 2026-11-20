"""CI-only network perimeter loaded by Python's site machinery.

Keep loopback/AF_UNIX working for in-process HTTP and Unix test fixtures.
Deny external DNS, IPv4/IPv6 connect and connect_ex before transport.
Never import this module in production or install it into src/.
"""
from __future__ import annotations

import ipaddress
import socket

_ORIG_GETADDRINFO = socket.getaddrinfo
_ORIG_CONNECT = socket.socket.connect
_ORIG_CONNECT_EX = socket.socket.connect_ex


def _local(host):
    if host is None:
        return True
    if isinstance(host, bytes):
        host = host.decode("ascii", "replace")
    host = str(host).strip().lower().strip("[]")
    if host in ("", "localhost", "localhost.localdomain", "0.0.0.0", "::"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _getaddrinfo(host, *args, **kwargs):
    if not _local(host):
        raise socket.gaierror(socket.EAI_NONAME, "OFFLINE_TEST: external hostname blocked")
    return _ORIG_GETADDRINFO(host, *args, **kwargs)


def _connect(sock, address):
    if sock.family != socket.AF_UNIX and (
        not isinstance(address, tuple) or not _local(address[0])
    ):
        raise OSError("OFFLINE_TEST: external socket connection blocked")
    return _ORIG_CONNECT(sock, address)


def _connect_ex(sock, address):
    if sock.family != socket.AF_UNIX and (
        not isinstance(address, tuple) or not _local(address[0])
    ):
        raise OSError("OFFLINE_TEST: external socket connection blocked")
    return _ORIG_CONNECT_EX(sock, address)


socket.getaddrinfo = _getaddrinfo
socket.socket.connect = _connect
socket.socket.connect_ex = _connect_ex
