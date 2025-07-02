import threading
import time
import uuid

from depeche_db._aggregated_stream import AggregatedStream
from depeche_db._executor import Executor
from depeche_db._interfaces import SubscriptionMessage
from depeche_db._message_handler import MessageHandlerRegister
from depeche_db._subscription import Subscription
from tests._account_example import Account, AccountEvent, AccountRepository
from tests.conftest import ACCOUNT1_ID


def test_it(pg_db, db_engine, stream_factory, store_factory, subscription_factory):
    store = store_factory()
    stream: AggregatedStream = stream_factory(store)

    handlers = MessageHandlerRegister[AccountEvent]()

    msg_sub_recv = []

    @handlers.register
    def handle_event_a(message: SubscriptionMessage[AccountEvent]):
        msg_sub_recv.append(time.time())

    subscription: Subscription = subscription_factory(stream, handlers=handlers)
    executors = []

    def projector():
        executor = Executor(db_dsn=pg_db, disable_signals=True, stimulation_interval=0)
        executor.register(stream.projector)
        executors.append(executor)
        executor.run()

    def sub():
        executor = Executor(db_dsn=pg_db, disable_signals=True, stimulation_interval=0)
        executor.register(subscription.runner)
        executors.append(executor)
        executor.run()

    try:
        threading.Thread(target=projector, daemon=True).start()
        threading.Thread(target=sub, daemon=True).start()
        time.sleep(0.5)

        account_repo = AccountRepository(store)
        account = Account.register(id=ACCOUNT1_ID, owner_id=uuid.uuid4(), number="123")
        account.credit(100)
        account_repo.save(account, expected_version=0)
        first_save_done = time.time()
        account.credit(100)
        account_repo.save(account, expected_version=2)
        second_save_done = time.time()
        time.sleep(0.5)

        assert len(msg_sub_recv) == 3
        # Timing
        print(msg_sub_recv[0] - first_save_done)
        print(msg_sub_recv[1] - second_save_done)
        print(msg_sub_recv[2] - second_save_done)

    finally:
        for executor in executors:
            executor._stop()
