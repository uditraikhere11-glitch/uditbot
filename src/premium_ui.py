"""Static presentation-only design system for authenticated AuditBot pages.

This module performs no broker, service, file-write, or configuration actions.
CSS/JS are inlined into existing authenticated HTML responses; no extra routes.
"""
from functools import lru_cache
from html import escape
from pathlib import Path

_ASSET_DIR = Path(__file__).resolve().parent
_PAGES = frozenset({'setup', 'control', 'designer'})


@lru_cache(maxsize=1)
def premium_styles() -> str:
    content = (_ASSET_DIR / 'premium_ui.css').read_text(encoding='utf-8')
    return '<style>\n' + content + '\n</style>'


@lru_cache(maxsize=1)
def premium_nav_js() -> str:
    return (_ASSET_DIR / 'premium_ui.js').read_text(encoding='utf-8')


def premium_nav(page: str) -> str:
    """Navigation to real authenticated pages/anchors; no fictional routes."""
    if page not in _PAGES:
        raise ValueError('Unknown premium UI page')
    rows = (
        ('control', '/', '⌂', 'Control Center'),
        ('setup', '/setup', '⚙', 'Setup &amp; Configuration'),
        ('orders', '/#amo-order-status', '▤', 'AMO Order Status'),
        ('health', '/#system-health', '▥', 'System Health'),
        ('designer', '/setup/telegram-designer', '✉', 'Telegram Message Designer'),
    )
    links = []
    for target, href, icon, title in rows:
        current = ' aria-current="page"' if target == page else ''
        links.append(f'<a href="{escape(href, quote=True)}"{current}><span aria-hidden="true">{icon}</span>{title}</a>')
    return (
        '<div id="premNavBackdrop" class="prem-nav-backdrop" hidden></div>\n'
        '<nav id="premNav" class="prem-nav" aria-label="AuditBot navigation" aria-hidden="true">\n'
        '<button id="premNavClose" type="button" class="prem-nav-close" aria-label="Close navigation">✕</button>\n'
        '<div class="prem-brand"><span class="prem-brand-icon" aria-hidden="true">↗</span>'
        '<div><strong>Audit<span>Bot</span></strong><small>TRADING SYSTEM · UDITBOT</small></div></div>\n'
        '<div class="prem-nav-group">WORKSPACE</div>\n'
        + '\n'.join(links[:4]) + '\n'
        '<div class="prem-nav-group">NOTIFICATIONS</div>\n'
        + links[4] + '\n'
        '<p class="prem-nav-foot">Navigation does not start trading or change orders. '
        'Check the Control Center for verified runtime status.</p>\n'
        '</nav>'
    )


@lru_cache(maxsize=1)
def premium_reference_styles() -> str:
    """Desktop-matched setup visual system; no data or business-logic reads."""
    css = (_ASSET_DIR / 'premium_reference.css').read_text(encoding='utf-8')
    return '<style>\n' + css + '\n</style>'


@lru_cache(maxsize=1)
def premium_reference_js() -> str:
    """Presentation-only Setup controls with fail-closed AMO API wrappers."""
    return (_ASSET_DIR / 'premium_reference.js').read_text(encoding='utf-8')
