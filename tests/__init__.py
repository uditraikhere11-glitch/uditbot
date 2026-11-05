"""
Offline Network-Isolation Safeguard for AuditBot Test Suite (P1-07).

Scope of protection:
- Enforces socket and DNS isolation inside the guarded Python `unittest` process
  by intercepting `socket.getaddrinfo`, `socket.gethostbyname`,
  `socket.gethostbyname_ex`, `socket.create_connection`, `socket.socket.connect`,
  and `socket.socket.connect_ex` for all non-loopback targets.
- Does not modify production modules (`src/`) or claim OS-level network
  isolation over arbitrary external child processes.
- Enforces attempt state clearing and swallowed-attempt detection across all
  `unittest` execution boundaries:
  1. `setUp` (`TestCase._callSetUp`)
  2. `testBody` (`TestCase._callTestMethod`)
  3. `tearDown` (`TestCase._callTearDown`)
  4. `addCleanup` callbacks (`TestCase._callCleanup` & `TestCase.doCleanups`)
  5. `setUpClass` (`TestSuite._handleClassSetUp`)
  6. `tearDownClass` (`TestSuite._tearDownPreviousClass`)
  7. `addClassCleanup` callbacks (`TestCase.doClassCleanups`)
  8. `setUpModule` (`TestSuite._handleModuleFixture`)
  9. `tearDownModule` (`TestSuite._handleModuleTearDown`)
  10. `addModuleCleanup` callbacks (`unittest.case.doModuleCleanups`)
"""

from __future__ import annotations

from contextlib import contextmanager
import socket
import sys
from typing import Any, Iterator, List, Tuple, Type
import unittest
import unittest.case
import unittest.suite
import unittest.util

_ORIG_GETADDRINFO = socket.getaddrinfo
_ORIG_GETHOSTBYNAME = socket.gethostbyname
_ORIG_GETHOSTBYNAME_EX = socket.gethostbyname_ex
_ORIG_CREATE_CONNECTION = socket.create_connection
_ORIG_SOCKET_CONNECT = socket.socket.connect
_ORIG_SOCKET_CONNECT_EX = socket.socket.connect_ex

_ORIG_TESTCASE_RUN = unittest.TestCase.run
_ORIG_CALL_SETUP = unittest.TestCase._callSetUp
_ORIG_CALL_TEST_METHOD = unittest.TestCase._callTestMethod
_ORIG_CALL_TEARDOWN = unittest.TestCase._callTearDown
_ORIG_CALL_CLEANUP = unittest.TestCase._callCleanup
_ORIG_DO_CLEANUPS = unittest.TestCase.doCleanups
_ORIG_DO_CLASS_CLEANUPS = unittest.TestCase.doClassCleanups
_ORIG_DO_MODULE_CLEANUPS = unittest.case.doModuleCleanups

_ORIG_HANDLE_CLASS_SETUP = unittest.suite.TestSuite._handleClassSetUp
_ORIG_TEARDOWN_PREVIOUS_CLASS = unittest.suite.TestSuite._tearDownPreviousClass
_ORIG_HANDLE_MODULE_FIXTURE = unittest.suite.TestSuite._handleModuleFixture
_ORIG_HANDLE_MODULE_TEARDOWN = unittest.suite.TestSuite._handleModuleTearDown

_LOOPBACK_HOSTS = frozenset({
    "127.0.0.1",
    "localhost",
    "::1",
    "0.0.0.0",
    "::",
    "",
})

_UNMOCKED_NETWORK_ATTEMPTS: List[Tuple[str, str]] = []
_ALLOW_BLOCKED_PROBE_DEPTH: int = 0
_GUARD_INSTALLED: bool = False


class OfflineNetworkIsolationError(socket.gaierror, RuntimeError):
    """Raised when an offline test attempts an unmocked external network or DNS operation."""


def is_loopback_target(host: Any) -> bool:
    if host is None:
        return True
    if isinstance(host, bytes):
        try:
            host = host.decode("utf-8", errors="ignore")
        except Exception:
            return False
    if not isinstance(host, str):
        return False
    cleaned = host.strip().lower()
    if cleaned in _LOOPBACK_HOSTS or cleaned.startswith("127."):
        return True
    return False


def _record_and_raise(api_name: str, target: Any) -> None:
    target_str = str(target)
    if _ALLOW_BLOCKED_PROBE_DEPTH <= 0:
        _UNMOCKED_NETWORK_ATTEMPTS.append((api_name, target_str))
    raise OfflineNetworkIsolationError(
        f"Unmocked external network call blocked in offline test suite: {api_name}({target_str!r})"
    )


def get_recorded_network_attempts() -> List[Tuple[str, str]]:
    return list(_UNMOCKED_NETWORK_ATTEMPTS)


def clear_recorded_network_attempts() -> None:
    _UNMOCKED_NETWORK_ATTEMPTS.clear()


@contextmanager
def allow_blocked_network_probe() -> Iterator[None]:
    """
    Context manager for negative tests that intentionally verify that
    `OfflineNetworkIsolationError` is raised on unmocked external network attempts
    without failing the enclosing TestCase.
    """
    global _ALLOW_BLOCKED_PROBE_DEPTH
    _ALLOW_BLOCKED_PROBE_DEPTH += 1
    try:
        yield
    finally:
        _ALLOW_BLOCKED_PROBE_DEPTH = max(0, _ALLOW_BLOCKED_PROBE_DEPTH - 1)


def _guarded_getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
    if not is_loopback_target(host):
        _record_and_raise("socket.getaddrinfo", f"{host}:{port}")
    return _ORIG_GETADDRINFO(host, port, *args, **kwargs)


def _guarded_gethostbyname(hostname: Any) -> str:
    if not is_loopback_target(hostname):
        _record_and_raise("socket.gethostbyname", hostname)
    return _ORIG_GETHOSTBYNAME(hostname)


def _guarded_gethostbyname_ex(hostname: Any) -> Any:
    if not is_loopback_target(hostname):
        _record_and_raise("socket.gethostbyname_ex", hostname)
    return _ORIG_GETHOSTBYNAME_EX(hostname)


def _guarded_create_connection(address: Any, *args: Any, **kwargs: Any) -> Any:
    host = address[0] if isinstance(address, tuple) and len(address) >= 1 else address
    if not is_loopback_target(host):
        _record_and_raise("socket.create_connection", address)
    return _ORIG_CREATE_CONNECTION(address, *args, **kwargs)


def _guarded_socket_connect(self: socket.socket, address: Any) -> Any:
    if isinstance(address, tuple) and len(address) >= 1:
        host = address[0]
        if not is_loopback_target(host):
            _record_and_raise("socket.socket.connect", address)
    return _ORIG_SOCKET_CONNECT(self, address)


def _guarded_socket_connect_ex(self: socket.socket, address: Any) -> int:
    if isinstance(address, tuple) and len(address) >= 1:
        host = address[0]
        if not is_loopback_target(host):
            _record_and_raise("socket.socket.connect_ex", address)
    return _ORIG_SOCKET_CONNECT_EX(self, address)


def _consume_and_fail_if_attempts(case: unittest.TestCase, phase: str) -> None:
    if _UNMOCKED_NETWORK_ATTEMPTS:
        attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
        _UNMOCKED_NETWORK_ATTEMPTS.clear()
        case.fail(
            f"Unmocked external network connection(s) attempted during {phase} of offline test "
            f"{case.id()}: {attempts}"
        )


def _issue_count(result: Any) -> int:
    if result is None:
        return 0
    return len(getattr(result, "failures", ())) + len(getattr(result, "errors", ()))


def _record_suite_exception(
    suite: unittest.suite.TestSuite,
    result: Any,
    phase_name: str,
    target_name: str,
    attempts: List[Tuple[str, str]],
) -> None:
    try:
        raise AssertionError(
            f"Unmocked external network connection(s) attempted during {phase_name} "
            f"of {target_name}: {attempts}"
        )
    except AssertionError as err:
        suite._createClassOrModuleLevelException(
            result,
            err,
            phase_name,
            target_name,
            info=sys.exc_info(),
        )


def install_offline_network_guard() -> None:
    global _GUARD_INSTALLED
    socket.getaddrinfo = _guarded_getaddrinfo
    socket.gethostbyname = _guarded_gethostbyname
    socket.gethostbyname_ex = _guarded_gethostbyname_ex
    socket.create_connection = _guarded_create_connection
    socket.socket.connect = _guarded_socket_connect
    socket.socket.connect_ex = _guarded_socket_connect_ex

    if not _GUARD_INSTALLED:
        _GUARD_INSTALLED = True

        def _guarded_run(self: unittest.TestCase, result: Any = None) -> Any:
            saved_outer_attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            try:
                return _ORIG_TESTCASE_RUN(self, result)
            finally:
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                _UNMOCKED_NETWORK_ATTEMPTS.extend(saved_outer_attempts)

        def _guarded_call_setup(self: unittest.TestCase) -> None:
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            try:
                _ORIG_CALL_SETUP(self)
            finally:
                _consume_and_fail_if_attempts(self, "setUp")

        def _guarded_call_test_method(self: unittest.TestCase, method: Any) -> None:
            try:
                _ORIG_CALL_TEST_METHOD(self, method)
            finally:
                _consume_and_fail_if_attempts(self, "testBody")

        def _guarded_call_teardown(self: unittest.TestCase) -> None:
            try:
                _ORIG_CALL_TEARDOWN(self)
            finally:
                _consume_and_fail_if_attempts(self, "tearDown")

        def _guarded_call_cleanup(self: unittest.TestCase, function: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return _ORIG_CALL_CLEANUP(self, function, *args, **kwargs)
            finally:
                _consume_and_fail_if_attempts(self, "addCleanup")

        def _guarded_do_cleanups(self: unittest.TestCase) -> bool:
            ok = _ORIG_DO_CLEANUPS(self)
            if _UNMOCKED_NETWORK_ATTEMPTS:
                attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                if getattr(self, "_outcome", None) is None:
                    self.fail(
                        f"Unmocked external network connection(s) attempted during doCleanups of offline test "
                        f"{self.id()}: {attempts}"
                    )
                return False
            return bool(ok)

        @classmethod
        def _guarded_do_class_cleanups(cls: Type[unittest.TestCase]) -> None:
            pre_existing = list(_UNMOCKED_NETWORK_ATTEMPTS)
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            cls.tearDown_exceptions = []
            try:
                while cls._class_cleanups:
                    function, args, kwargs = cls._class_cleanups.pop()
                    try:
                        try:
                            function(*args, **kwargs)
                        finally:
                            if _UNMOCKED_NETWORK_ATTEMPTS:
                                attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
                                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                                raise AssertionError(
                                    f"Unmocked external network connection(s) attempted during addClassCleanup "
                                    f"of {unittest.util.strclass(cls)}: {attempts}"
                                )
                    except Exception:
                        cls.tearDown_exceptions.append(sys.exc_info())
            finally:
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                _UNMOCKED_NETWORK_ATTEMPTS.extend(pre_existing)

        def _guarded_do_module_cleanups() -> None:
            pre_existing = list(_UNMOCKED_NETWORK_ATTEMPTS)
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            exceptions: List[BaseException] = []
            try:
                while unittest.case._module_cleanups:
                    function, args, kwargs = unittest.case._module_cleanups.pop()
                    try:
                        try:
                            function(*args, **kwargs)
                        finally:
                            if _UNMOCKED_NETWORK_ATTEMPTS:
                                attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
                                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                                raise AssertionError(
                                    f"Unmocked external network connection(s) attempted during addModuleCleanup: "
                                    f"{attempts}"
                                )
                    except Exception as exc:
                        exceptions.append(exc)
                if exceptions:
                    raise exceptions[0]
            finally:
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                _UNMOCKED_NETWORK_ATTEMPTS.extend(pre_existing)

        def _guarded_handle_class_setup(self: unittest.suite.TestSuite, test: Any, result: Any) -> None:
            previous_class = getattr(result, "_previousTestClass", None)
            current_class = test.__class__
            will_run = (
                current_class != previous_class
                and not getattr(result, "_moduleSetUpFailed", False)
                and not getattr(current_class, "__unittest_skip__", False)
            )
            if not will_run:
                return _ORIG_HANDLE_CLASS_SETUP(self, test, result)

            saved_outer = list(_UNMOCKED_NETWORK_ATTEMPTS)
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            before_issues = _issue_count(result)
            try:
                _ORIG_HANDLE_CLASS_SETUP(self, test, result)
                if _UNMOCKED_NETWORK_ATTEMPTS:
                    attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
                    _UNMOCKED_NETWORK_ATTEMPTS.clear()
                    if _issue_count(result) == before_issues and not getattr(current_class, "_classSetupFailed", False):
                        try:
                            current_class._classSetupFailed = True
                        except TypeError:
                            pass
                        class_name = unittest.util.strclass(current_class)
                        _record_suite_exception(self, result, "setUpClass", class_name, attempts)
                        do_class_cleanups = getattr(current_class, "doClassCleanups", None)
                        if do_class_cleanups is not None:
                            do_class_cleanups()
                            for exc_info in getattr(current_class, "tearDown_exceptions", ()):
                                self._createClassOrModuleLevelException(
                                    result, exc_info[1], "setUpClass", class_name, info=exc_info
                                )
            finally:
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                _UNMOCKED_NETWORK_ATTEMPTS.extend(saved_outer)

        def _guarded_tear_down_previous_class(self: unittest.suite.TestSuite, test: Any, result: Any) -> None:
            previous_class = getattr(result, "_previousTestClass", None)
            current_class = test.__class__
            will_run = (
                previous_class is not None
                and current_class != previous_class
                and not getattr(previous_class, "_classSetupFailed", False)
                and not getattr(result, "_moduleSetUpFailed", False)
                and not getattr(previous_class, "__unittest_skip__", False)
            )
            if not will_run:
                return _ORIG_TEARDOWN_PREVIOUS_CLASS(self, test, result)

            saved_outer = list(_UNMOCKED_NETWORK_ATTEMPTS)
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            before_issues = _issue_count(result)
            try:
                _ORIG_TEARDOWN_PREVIOUS_CLASS(self, test, result)
                if _UNMOCKED_NETWORK_ATTEMPTS:
                    attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
                    _UNMOCKED_NETWORK_ATTEMPTS.clear()
                    class_name = unittest.util.strclass(previous_class)
                    teardown_exc_count = len(getattr(previous_class, "tearDown_exceptions", ()))
                    if (_issue_count(result) - before_issues) <= teardown_exc_count:
                        _record_suite_exception(self, result, "tearDownClass", class_name, attempts)
            finally:
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                _UNMOCKED_NETWORK_ATTEMPTS.extend(saved_outer)

        def _guarded_handle_module_fixture(self: unittest.suite.TestSuite, test: Any, result: Any) -> None:
            previous_module = self._get_previous_module(result)
            current_module = test.__class__.__module__
            if current_module == previous_module:
                return _ORIG_HANDLE_MODULE_FIXTURE(self, test, result)

            saved_outer = list(_UNMOCKED_NETWORK_ATTEMPTS)
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            try:
                # Tear down previous module first using guarded _handleModuleTearDown
                self._handleModuleTearDown(result)
                result._moduleSetUpFailed = False
                try:
                    module = sys.modules[current_module]
                except KeyError:
                    return
                set_up_module = getattr(module, "setUpModule", None)
                if set_up_module is not None:
                    unittest.suite._call_if_exists(result, "_setupStdout")
                    try:
                        try:
                            set_up_module()
                        except Exception as exc:
                            if isinstance(result, unittest.suite._DebugResult):
                                raise
                            result._moduleSetUpFailed = True
                            self._createClassOrModuleLevelException(result, exc, "setUpModule", current_module)
                        if _UNMOCKED_NETWORK_ATTEMPTS:
                            attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
                            _UNMOCKED_NETWORK_ATTEMPTS.clear()
                            if not result._moduleSetUpFailed:
                                result._moduleSetUpFailed = True
                                _record_suite_exception(self, result, "setUpModule", current_module, attempts)
                        if result._moduleSetUpFailed:
                            try:
                                unittest.case.doModuleCleanups()
                            except Exception as exc:
                                self._createClassOrModuleLevelException(result, exc, "setUpModule", current_module)
                    finally:
                        unittest.suite._call_if_exists(result, "_restoreStdout")
            finally:
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                _UNMOCKED_NETWORK_ATTEMPTS.extend(saved_outer)

        def _guarded_handle_module_teardown(self: unittest.suite.TestSuite, result: Any) -> None:
            previous_module = self._get_previous_module(result)
            if previous_module is None or getattr(result, "_moduleSetUpFailed", False):
                return _ORIG_HANDLE_MODULE_TEARDOWN(self, result)

            saved_outer = list(_UNMOCKED_NETWORK_ATTEMPTS)
            _UNMOCKED_NETWORK_ATTEMPTS.clear()
            before_issues = _issue_count(result)
            try:
                _ORIG_HANDLE_MODULE_TEARDOWN(self, result)
                if _UNMOCKED_NETWORK_ATTEMPTS:
                    attempts = list(_UNMOCKED_NETWORK_ATTEMPTS)
                    _UNMOCKED_NETWORK_ATTEMPTS.clear()
                    if _issue_count(result) == before_issues:
                        _record_suite_exception(self, result, "tearDownModule", previous_module, attempts)
            finally:
                _UNMOCKED_NETWORK_ATTEMPTS.clear()
                _UNMOCKED_NETWORK_ATTEMPTS.extend(saved_outer)

        unittest.TestCase.run = _guarded_run  # type: ignore[method-assign]
        unittest.TestCase._callSetUp = _guarded_call_setup  # type: ignore[method-assign]
        unittest.TestCase._callTestMethod = _guarded_call_test_method  # type: ignore[method-assign]
        unittest.TestCase._callTearDown = _guarded_call_teardown  # type: ignore[method-assign]
        unittest.TestCase._callCleanup = _guarded_call_cleanup  # type: ignore[method-assign]
        unittest.TestCase.doCleanups = _guarded_do_cleanups  # type: ignore[method-assign]
        unittest.TestCase.doClassCleanups = _guarded_do_class_cleanups  # type: ignore[method-assign]
        unittest.case.doModuleCleanups = _guarded_do_module_cleanups
        unittest.suite.case.doModuleCleanups = _guarded_do_module_cleanups
        unittest.suite.TestSuite._handleClassSetUp = _guarded_handle_class_setup  # type: ignore[method-assign]
        unittest.suite.TestSuite._tearDownPreviousClass = _guarded_tear_down_previous_class  # type: ignore[method-assign]
        unittest.suite.TestSuite._handleModuleFixture = _guarded_handle_module_fixture  # type: ignore[method-assign]
        unittest.suite.TestSuite._handleModuleTearDown = _guarded_handle_module_teardown  # type: ignore[method-assign]


install_offline_network_guard()
