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


@lru_cache(maxsize=1)
def premium_replica_js() -> str:
    return (_ASSET_DIR / 'premium_replica.js').read_text(encoding='utf-8')


def premium_nav(page: str) -> str:
    """Navigation only; hrefs are known authenticated routes, not user input."""
    if page not in _PAGES:
        raise ValueError('Unknown premium UI page')
    rows = (
        ('control', '/', '⌂', 'Control Center'),
        ('setup', '/setup', '⚙', 'Setup &amp; Configuration'),
        ('designer', '/setup/telegram-designer', '✉', 'Telegram Message Designer'),
    )
    links = []
    for target, href, icon, title in rows:
        current = ' aria-current="page"' if target == page else ''
        links.append(f'<a href="{escape(href, quote=True)}"{current}><span aria-hidden="true">{icon}</span>{title}</a>')
    return '''<div id="premNavBackdrop" class="prem-nav-backdrop" hidden></div>
<nav id="premNav" class="prem-nav" aria-label="AuditBot navigation" aria-hidden="true">
  <button id="premNavClose" type="button" class="prem-nav-close" aria-label="Close navigation">✕</button>
  <div class="prem-brand"><span class="prem-brand-icon" aria-hidden="true">↗</span>
    <div><strong>Audit<span>Bot</span></strong><small>TRADING SYSTEM · UDITBOT</small></div></div>
  <div class="prem-nav-group">WORKSPACE</div>
  ''' + links[0] + '\n  ' + links[1] + '''
  <div class="prem-nav-group">NOTIFICATIONS</div>
  ''' + links[2] + '''
  <p class="prem-nav-foot">Navigation does not start trading or change orders. Check the Control Center for verified runtime status.</p>
</nav>'''