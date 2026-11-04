# TODO — remaining work

Work to accomplish.  Delete from this file when the change is ready to 
commit.

## Queued-case disconnect (optional, not P0)

If a client disconnects while *waiting* in `_enter_gate`, the handler still
blocks up to `queue_timeout_seconds` (300s default). Simple improvement:
poll `select`-based timeout in 0.5s slices in the semaphore wait and bail
with 503 if the client died.

## show status

Use our existing tables to expand on what is currently running and what is
queued.

## root-path customization

Right now we always serve "OK" -- move this to a config.json setting.
