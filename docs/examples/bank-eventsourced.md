# Example: Bank

A bank account is classic example when event sourcing is shown.

## Repo

[https://github.com/depeche-py/example-bank](https://github.com/depeche-py/example-bank)

## Walk through

### `bank/api`

A simple REST API using FastAPI


### [`bank/messages.py`](https://github.com/depeche-py/example-bank/blob/main/bank/messages.py)

Defines a base class for all messages in the system as well as a couple of command and event messages, e.g.

```python
class CreateAccountCommand(Message):
    account_id: _uuid.UUID | None = None
    account_number: str


class AccountCreatedEvent(Message):
    account_id: _uuid.UUID
    account_number: str
```


### [`bank/domain.py`](https://github.com/depeche-py/example-bank/blob/main/bank/domain.py)

The definition of the Domain objects

#### `Account`

Account has three operations (create, withdraw, deposit) and tracks the
resulting balance.

The operations are implemented in such a way that they just append & apply a
new event. The `_apply` method is called by the inherited `apply` method. It is
the projection that will be applied for each event whenever the state needs to
be (re)constructed from them.

```python
class Account(_es.EventSourcedAggregateRoot[_uuid.UUID, _messages.AccountEvent]):
    id: _uuid.UUID
    number: str
    balance: int

    def _apply(self, event: _messages.AccountEvent) -> None:
        if isinstance(event, _messages.AccountCreatedEvent):
            self.id = event.account_id
            self.number = event.account_number
            self.balance = 0
        # over event types...

    @classmethod
    def create(cls, account_id: _uuid.UUID, number: str) -> "Account":
        account = cls()
        account.apply(
            _messages.AccountCreatedEvent(account_id=account_id, account_number=number)
        )
        return account

    #...
```

#### `Transfer`

Is used to keep the state for transfer from one to another `Account`. Together
with the event handlers it can be seen as sort of a process manager.


### `bank/handlers`

#### [`commands`](https://github.com/depeche-py/example-bank/blob/main/bank/handlers/commands.py)

Defines a class that handles command messages. It uses repositories to load/save
the event-sourced domain objects. The command handler is used both from the
REST API and from a subscription on a stream of command messages.

#### [`events`](https://github.com/depeche-py/example-bank/blob/main/bank/handlers/events.py)

Defines classes that handle events. The event handlers are used by subscriptions
to event streams.

#### [`queries`](https://github.com/depeche-py/example-bank/blob/main/bank/handlers/queries.py)

Defines a class that is used by the REST API to get domain objects.


### [`bank/infra`](https://github.com/depeche-py/example-bank/blob/main/bank/infra.py)

In this file, all the infrastructure is defined. Most notably, we define:

- a dependency injection container (using the [`explicit-di`
library](https://github.com/depeche-py/explicit-di) which is used to simplify
the creation of & calls to the handler objects.
- aggregate streams and subscriptions
