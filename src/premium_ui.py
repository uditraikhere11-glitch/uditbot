"""Static presentation-only design system for authenticated AuditBot pages.

This module performs no broker, service, file-write, or configuration actions.
CSS/JS are inlined into existing authenticated HTML responses; no extra routes.
"""
from functools import lru_cache
from html import escape
from pathlib import Path
from urllib.parse import quote

_ASSET_DIR = Path(__file__).resolve().parent
_PAGES = frozenset({'setup', 'control', 'designer'})

# AuditBot is the shared trading system. The logo never varies by VM/user.
# UditBot/AmanBot remain configured instance labels, not separate brands.
# Owner-approved AuditBot mark: symbol extracted from the supplied logo artwork.
# The white/blue/green strokes remain unchanged, stored as a compact PNG data URI.
_AUDITBOT_IMAGE_B64 = (
    "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"
)
_AUDITBOT_MARK = (
    '<svg class="v17p-audit-mark" xmlns="http://www.w3.org/2000/svg" '
    'viewBox="0 0 112 112" aria-hidden="true" focusable="false">'
    '<rect class="v17p-audit-tile" x="0" y="0" width="112" height="112" '
    'rx="23" fill="#071727"/>'
    '<image x="0" y="0" width="112" height="112" '
    'href="data:image/png;base64,' + _AUDITBOT_IMAGE_B64 + '"/>'
    '</svg>'
)
_AUDITBOT_FAVICON = quote(_AUDITBOT_MARK, safe='')


def premium_instance_name() -> str:
    """Read a display-only instance label from the local saved configuration.

    Never substitute arbitrary environment secrets into authenticated HTML.
    Each render re-reads the saved instance, keeping UditBot and AmanBot distinct.
    """
    from src.config import parse_env_dict

    saved = parse_env_dict('.env')
    for key in ('OPERATOR_NAME', 'BOT_INSTANCE_NAME'):
        candidate = str(saved.get(key, '')).strip()
        if candidate:
            # Limit pathological labels while preserving ordinary names. HTML
            # escaping happens at the actual output point, never in .env.
            return candidate[:96]
    return 'UDITBOT'


def premium_configured_dhan_label() -> str:
    """Masked saved account identifier, not a broker connectivity assertion.

    This function intentionally reads the configured DHAN_CLIENT_ID only.
    An authenticated Dhan profile call is required to claim connectivity.
    """
    from src.config import parse_env_dict

    account = str(parse_env_dict('.env').get('DHAN_CLIENT_ID', '')).strip()
    if not account:
        return 'Not configured'
    if len(account) < 4 or not account.isascii() or not account.isdigit():
        return 'Configured'
    return '***' + account[-4:]


@lru_cache(maxsize=1)
def premium_styles() -> str:
    content = (_ASSET_DIR / 'premium_ui.css').read_text(encoding='utf-8')
    return ('<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,'
            + _AUDITBOT_FAVICON + '">\n<style>\n' + content + '\n</style>')


@lru_cache(maxsize=1)
def premium_nav_js() -> str:
    return (_ASSET_DIR / 'premium_ui.js').read_text(encoding='utf-8')


def premium_nav(page: str) -> str:
    """Shared preview-style top/bottom navigation with original drawer retained.

    All destinations are the existing authenticated routes or presentation
    fragments. No new broker, service, or account-state actions are added.
    """
    if page not in _PAGES:
        raise ValueError('Unknown premium UI page')
    raw_name = premium_instance_name()
    name = escape(raw_name, quote=True)
    public_brand = raw_name if raw_name.casefold().endswith('bot') else raw_name + 'Bot'
    brand = escape(public_brand, quote=True)
    initial = escape(raw_name[:1].upper() or 'A', quote=True)
    # Never render a complete client ID or describe a saved identifier as CONNECTED.
    dhan_label = escape(premium_configured_dhan_label(), quote=True)
    destinations = (
        ('dashboard', '/#dashboard', '⌂', 'Dashboard'),
        ('control', '/#control', '▷', 'Control'),
        ('setup', '/setup', '⚙', 'Setup'),
        ('telegram', '/setup/telegram-designer', '➤', 'Telegram'),
        ('amo', '/#amo', '▣', 'AMO'),
        ('health', '/#health', '♡', 'Health'),
    )
    def links(classes: str, *, initial_current: bool = False) -> str:
        rows = []
        for key, href, glyph, label in destinations:
            selected = ((page == 'control' and key == 'dashboard') or
                        (page == 'setup' and key == 'setup') or
                        (page == 'designer' and key == 'telegram'))
            current = ' aria-current="page"' if selected and initial_current else ''
            rows.append(
                f'<a class="{classes}" data-v17p-dest="{key}" href="{escape(href, quote=True)}"{current}>'
                f'<span aria-hidden="true">{glyph}</span><span>{label}</span></a>'
            )
        return '\n'.join(rows)
    return f"""<header class="v17p-topbar" aria-label="AuditBot application header">
  <a class="v17p-brand" href="/" aria-label="AuditBot dashboard, {brand} instance"><span aria-hidden="true">{_AUDITBOT_MARK}</span><span class="v17p-brand-name">AuditBot</span></a>
  <nav class="v17p-desktop-tabs" aria-label="Main navigation">{links('v17p-top-link', initial_current=True)}</nav>
  <div class="v17p-top-actions">
    <span class="v17p-dhan-badge" aria-label="Saved Dhan account: {dhan_label}; connection not verified here" title="Saved account identifier only; verify connectivity in Dashboard or Health">Dhan <strong>{dhan_label}</strong></span>
    <span class="v17p-instance-label" title="Configured bot instance: {brand}">{brand}</span>
    <span id="v17p-runtime-chip" class="v17p-runtime-chip" aria-live="polite">UNVERIFIED</span>
    <button type="button" id="premiumThemeToggle" class="v17p-theme-btn" aria-label="Toggle light or dark appearance" aria-pressed="false">☼</button>
    <button type="button" id="v17pAccountOpen" class="v17p-account" aria-label="Open {brand} account and navigation menu" aria-controls="premNav" aria-expanded="false">{initial}</button>
  </div>
</header>
<div id="premNavBackdrop" class="prem-nav-backdrop" hidden></div>
<nav id="premNav" class="prem-nav" aria-label="AuditBot navigation" aria-hidden="true">
  <button id="premNavClose" type="button" class="prem-nav-close" aria-label="Close navigation">✕</button>
  <div class="prem-brand"><span class="prem-brand-icon" aria-hidden="true">{_AUDITBOT_MARK}</span>
    <div><strong>{brand}</strong><small>Powered by AuditBot · Trading System</small></div></div>
  <div class="prem-nav-group">WORKSPACE</div>
  <a href="/#dashboard"><span aria-hidden="true">⌂</span>Dashboard</a>
  <a href="/#control"><span aria-hidden="true">▷</span>Trading Controls</a>
  <a href="/setup"><span aria-hidden="true">⚙</span>Setup &amp; Configuration</a>
  <a href="/setup/telegram-designer"><span aria-hidden="true">➤</span>Telegram Designer</a>
  <a href="/#amo"><span aria-hidden="true">▣</span>AMO Orders</a>
  <a href="/#health"><span aria-hidden="true">♡</span>System Health</a>
  <p class="prem-nav-foot">Only verified statuses should be trusted; navigating does not place trades.</p>
</nav>
<nav class="v17p-bottom-tabs" aria-label="Mobile main navigation">{links('v17p-bottom-link')}</nav>"""
