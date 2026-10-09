"""Recover durable safety evidence in order, without replaying any work."""
from .ports import ActionLedgerPort, RequestLedgerPort


def recover_actions_and_requests(actions: ActionLedgerPort | None, requests: RequestLedgerPort, occurred_at: str) -> None:
    """Reconciliation is repeatable; persisted evidence never authorizes replay."""
    try:
        if actions is not None:
            actions.recover_unresolved(occurred_at)
            committed = actions.committed_request_ids()
        else:
            committed = ()
        requests.recover_interrupted(committed)
    except Exception:
        if actions is not None:
            actions.degrade()
        raise
