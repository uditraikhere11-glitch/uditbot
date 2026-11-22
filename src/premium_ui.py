"""Static presentation-only design system for authenticated AuditBot pages.

This module performs no broker, service, file-write, or configuration actions.
CSS/JS are inlined into existing authenticated HTML responses; no extra routes.
"""
from functools import lru_cache
from html import escape
from pathlib import Path

_ASSET_DIR = Path(__file__).resolve().parent
_PAGES = frozenset({'setup', 'control', 'designer'})


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
    return '<style>\n' + content + '\n</style>'


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
  <a class="v17p-brand" href="/" aria-label="{brand} dashboard"><span aria-hidden="true">⌁</span>{brand}</a>
  <nav class="v17p-desktop-tabs" aria-label="Main navigation">{links('v17p-top-link', initial_current=True)}</nav>
  <div class="v17p-top-actions">
    <span class="v17p-dhan-badge" aria-label="Saved Dhan account: {dhan_label}; connection not verified here" title="Saved account identifier only; verify connectivity in Dashboard or Health">Dhan <strong>{dhan_label}</strong></span>
    <span id="v17p-runtime-chip" class="v17p-runtime-chip" aria-live="polite">UNVERIFIED</span>
    <button type="button" id="premiumThemeToggle" class="v17p-theme-btn" aria-label="Toggle light or dark appearance" aria-pressed="false">☼</button>
    <button type="button" id="v17pAccountOpen" class="v17p-account" aria-label="Open account and navigation menu" aria-controls="premNav" aria-expanded="false">{initial}</button>
  </div>
</header>
<div id="premNavBackdrop" class="prem-nav-backdrop" hidden></div>
<nav id="premNav" class="prem-nav" aria-label="AuditBot navigation" aria-hidden="true">
  <button id="premNavClose" type="button" class="prem-nav-close" aria-label="Close navigation">✕</button>
  <div class="prem-brand"><span class="prem-brand-icon" aria-hidden="true">⌁</span>
    <div><strong>Audit<span>Bot</span></strong><small>TRADING SYSTEM · {name}</small></div></div>
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
