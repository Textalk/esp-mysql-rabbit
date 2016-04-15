Event Store Promises using MySQL and RabbitMQ
=============================================

Aiming to be a drop in replacement (or pre-placement, for careful sysops not wanting to add
[Event Store](https://geteventstore.com/) to the stack yet) of
[event-store-promise](https://github.com/Textalk/event-store-promise).  It should keep true to the
exact same API.


Using it in your project
------------------------

Install:

```bash
npm install --save @textalk/esp-mysql-rabbit
cat db.sql | mysql -uesp -pesp -Desp
```

Connect:

```
const esp = await(EventStorePromise({
  mysql: {
    user:     'esp',
    password: 'esp',
    database: 'esp',
  },
  amqp: {
    url:      'amqp://localhost',
    exchange: 'events',
  }
}, {timeout: 10000}))
```

MySQL: For transactions to work optimally, the database should NOT have `READ COMMITTED`.

AMQP: The exchange will be asserted as a durable `topic`-exchange.

See `examples/readWriting.js` for a working example (provided mysql and amqp-servers are in
expected place).
