from depeche_db import Subscription
from depeche_db.tools import DbSubscriptionStateProvider


def test_ack_in_client_transaction(
    identifier, db_engine, stream_with_events, subscription_factory
):
    subject: Subscription = subscription_factory(
        stream_with_events,
        DbSubscriptionStateProvider(name=identifier(), engine=db_engine),
    )

    first_event_id = None
    with db_engine.connect() as conn:
        with conn.begin() as tx:
            for event in subject.get_next_messages(count=1):
                first_event_id = event.stored_message.message_id
                event.ack.execute(conn=conn)
            tx.rollback()

    # we rolled back, so we should get the same event again
    seen_event_id = None
    for event in subject.get_next_messages(count=1):
        seen_event_id = event.stored_message.message_id
        break
    assert seen_event_id == first_event_id
