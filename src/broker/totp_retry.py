"""Bounded retry for a scheduled Dhan login rejected as Invalid TOTP.

This function does not generate a second token on HTTP 401, account mismatch,
or other broker errors. The caller already owns the shared Dhan token lock;
a runtime/on-demand authentication attempt must never wait for a new code slot.
"""

import logging
import time

logger = logging.getLogger(__name__)


def retry_scheduled_invalid_totp_once(
    response, *, on_demand, session, auth_url, params, timeout, generate_code,
    clock=time.time, sleeper=time.sleep,
):
    """Retry an explicit broker Invalid TOTP once in a different 30s slot.

    Return the original response on any non-matching error. A failed second
    response is returned for the caller's existing fail-closed verification.
    A transport exception on the second request propagates to its existing
    error handler. No token is published here.
    """
    if on_demand:
        return response
    try:
        body = response.json()
    except (ValueError, TypeError, AttributeError):
        return response
    if not isinstance(body, dict):
        return response
    if (
        str(body.get("status", "")).strip().lower() != "error"
        or str(body.get("message", "")).strip().lower() != "invalid totp"
        or body.get("accessToken")
    ):
        return response

    now = float(clock())
    # Compute a fresh TOTP only after crossing the next 30-second boundary.
    # The 0.75s cushion permits minor time/transport skew.
    next_slot = (int(now // 30) + 1) * 30.0 + 0.75
    pause = max(0.0, next_slot - now)
    logger.warning("Dhan rejected scheduled TOTP; retrying once in next 30-second TOTP window")
    sleeper(pause)
    retry_params = dict(params)
    retry_params["totp"] = generate_code()
    return session.post(auth_url, params=retry_params, timeout=timeout)
