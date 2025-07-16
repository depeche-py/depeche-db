from depeche_db import Subscription
from depeche_db.tools import DbSubscriptionStateProvider


def test_ack_in_client_transaction(
    identifier, db_engine, stream_with_events, subscription_factory
):
    subject: Subscription = subscription_factory(
        stream_with_events,
        DbSubscriptionStateProvider(name=identifier(), engine=db_engine),
    )

    with db_engine.connect() as conn:
        with conn.begin() as tx:
            for event in subject.get_next_messages(count=1):
                event.ack.execute(conn=conn)
            tx.rollback()

    # we rolled back, so the state should be empty
    state = subject._state_provider.read(subject.name)
    assert state.positions == {}
