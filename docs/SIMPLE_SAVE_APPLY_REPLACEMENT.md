# Simple Save & Apply

Setup and the two Control Center AMO toggles use the same synchronous settings writer. A matching revision and valid fields permit an atomic edit. An unchanged submission issues no restart. An owner-stopped service saves settings for its next start.

A running service needs a registered broker handoff, an empty mutation ledger, matching runtime evidence and the applicable LIVE confirmation. The writer takes the broker fence before saving and requests one systemd restart. A verified replacement PID, configuration hash, credential fingerprint and broker registration allow release. The WebUI then displays the result and queues the same message in the existing Telegram log-group outbox. A Telegram outage cannot retry the restart. Existing deployments must retain the broker handoff producer setting.

The former async settings submission, coordinator tick and deferred-cancel routes are retired. Previous records remain available for diagnosis. Save & Apply archives a completed V2 record only when its saved revision matches. An interrupted PREPARED operation requires the original file revision and explicit evidence that restart was never issued. An active fence additionally requires matching account, operation and producer identity and an empty broker ledger. Other uncertain operations block the save. Never delete a barrier to resume trading.
