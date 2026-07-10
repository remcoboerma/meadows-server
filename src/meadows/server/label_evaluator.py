"""Label subscription evaluator — matches predicates against labels.

BUSINESS RULE (MEADOWS-labeling-intent §2.1): labels are how messages
reach abonnees.  The server evaluates JSON Logic predicates against
each label on a message and returns which subscriptions matched.

Uses meadows.jsonlogic.evaluate for the actual predicate evaluation.
"""

from __future__ import annotations

from typing import Any

from meadows.jsonlogic import evaluate
from meadows.protocol import Message


def evaluate_label_subscriptions(
    subscriptions: list[dict[str, Any]],
    labels: list[dict[str, Any]],
    _message: Message,
) -> dict[str, list[dict[str, Any]]]:
    """Evaluate all subscriptions against a message's labels.

    Args:
        subscriptions: list of subscription dicts with keys:
            name, predicate, deliver, scope, group_id
        labels: list of label dicts (wire form: origin, label, semver, metadata)
        message: the Message (for future context access)

    Returns:
        {subscription_name: [matched_label_dict, ...]}
    """
    matches: dict[str, list[dict[str, Any]]] = {}
    for sub in subscriptions:
        predicate = sub.get("predicate", {})
        sub_name = sub.get("name", "")
        matched = []
        for lbl in labels:
            if evaluate(predicate, lbl):
                matched.append(lbl)
        if matched:
            matches[sub_name] = matched
    return matches
