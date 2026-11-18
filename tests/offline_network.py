"""Limit the Linux CI test process tree to local Unix sockets."""
import ctypes
import ctypes.util
import errno
import socket


def block_network():
    library = ctypes.CDLL(ctypes.util.find_library("seccomp") or "libseccomp.so.2")

    class Comparison(ctypes.Structure):
        _fields_ = [("argument", ctypes.c_uint), ("operator", ctypes.c_int),
                    ("first", ctypes.c_uint64), ("second", ctypes.c_uint64)]

    library.seccomp_init.argtypes = [ctypes.c_uint32]
    library.seccomp_init.restype = ctypes.c_void_p
    library.seccomp_rule_add_array.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint, ctypes.POINTER(Comparison),
    ]
    library.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    library.seccomp_load.argtypes = library.seccomp_release.argtypes = [ctypes.c_void_p]
    context = library.seccomp_init(0x7fff0000)  # SCMP_ACT_ALLOW
    if not context:
        raise RuntimeError("Could not initialize offline test isolation")
    try:
        non_unix = Comparison(0, 1, socket.AF_UNIX, 0)  # SCMP_CMP_NE
        deny = 0x50000 | errno.EACCES  # SCMP_ACT_ERRNO
        for name in (b"socket", b"socketpair"):
            number = library.seccomp_syscall_resolve_name(name)
            if number < 0 or library.seccomp_rule_add_array(context, deny, number, 1, ctypes.byref(non_unix)):
                raise RuntimeError("Could not configure offline test isolation")
        if library.seccomp_load(context):
            raise RuntimeError("Could not activate offline test isolation")
    finally:
        library.seccomp_release(context)
