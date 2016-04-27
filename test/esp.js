'use strict'

const Stream  = require('stream')
const assert  = require('assert')
const aawait  = require('asyncawait/await')
const aasync  = require('asyncawait/async')
const sinon   = require('sinon')
const Esp     = require('../esp')
const _       = require('highland')
const await_  = stream => aawait(new Promise((res, rej) => stream.errors(rej).toArray(res)))
const Promise = require('bluebird') // At least until we have array destructuring!

describe('esp mysql rabbit', () => {
  describe('Esp factory', () => {
    it('should connect to mysql and amqp, returning esp', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {my: 'mysql-options'},
        amqp:     {my: 'amqp-options', exchange: 'events'},
        mysqlLib: {createConnection: options => {
          assert.deepEqual(options, {dateStrings: true, my: 'mysql-options'})
          return {fingerprint: 'my-mysql', connect: (cb) => cb(), query: () => {}}
        }},
        amqpLib:  {connect: options => Promise.resolve('myAmqpConn')}
      }))

      assert.equal(esp.mysqlConn.fingerprint, 'my-mysql')
      assert.equal(esp.amqpConn, 'myAmqpConn')
    }))
  })

  describe('esp.close()', () => {
    it('should close connections', aasync(() => {
      let mysqlEndCallCount = 0
      const mysqlEnd = cb => {mysqlEndCallCount++; cb()}

      const amqpClose = sinon.stub()
      amqpClose.returns(Promise.resolve());

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          end:     mysqlEnd,
          query:   () => {},
        })},
        amqpLib:  {connect: options => Promise.resolve({close: amqpClose})}
      }))

      aawait(esp.close())

      assert.equal(mysqlEndCallCount, 1)
      assert.equal(amqpClose.callCount, 1)
    }))
  })

  describe('esp.createGuid()', () => {
    it('should give a string', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {},
        mysqlLib: {createConnection: options => ({connect: cb => cb(), query: () => {}})},
        amqpLib:  {connect: options => Promise.resolve()},
      }))

      assert.equal(typeof esp.createGuid(), 'string')
    }))
  })

  describe('esp.ping()', () => {
    it('should promise a pong', aasync(() => {
      let mysqlPingCount = 0

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          ping:    cb => {mysqlPingCount++; cb()},
          query:   () => {},
        })},
        amqpLib:  {connect: options => Promise.resolve()},
      }))

      aawait(esp.ping())

      assert.equal(mysqlPingCount, 1)
    }))
  })

  describe('esp.subscribeToStream()', () => {
    it('should get new events when subscribed', aasync(() => {
      let consumeQueue         = null
      let assertExchangeCalled = false
      let usedRoutingKey       = null

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({connect: cb => cb(), query: () => {}})},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(assertExchangeCalled = true),
          assertQueue:    (queue, options)              => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange, routingKey) => Promise.resolve(
            usedRoutingKey = routingKey
          ),
          consume:        (queue, cb, options)          => {
            consumeQueue = queue
            process.nextTick(() => cb({
              content: new Buffer(JSON.stringify({
                streamId:  'mystream',
                eventType: 'myEventType',
                updated:   '1970-01-01T12:13:42.492473Z',
                data:      {my: 'eventdata'},
              }))
            }))
            return Promise.resolve()
          }
        })})}
      }))

      const event = aawait(new Promise((resolve, reject) => {
        const stream = esp.subscribeToStream('mystream')
        stream.on('data', event => resolve(event))
      }))

      assert(assertExchangeCalled)
      assert.equal(consumeQueue,    'foo')
      assert.equal(usedRoutingKey,  'mystream')
      assert.equal(event.streamId,  'mystream')
      assert.equal(event.eventType, 'myEventType')
      assert.deepEqual(event.data,  {my: 'eventdata'}, 'Event data should be parsed')
    }))

    it('should close on maxCount', aasync(() => {
      let closeCalled = false

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({connect: cb => cb(), query: () => {}})},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(),
          assertQueue:    (queue, options)  => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange) => Promise.resolve(),
          consume:        (queue, cb, options)  => {
            return Promise.resolve()
              .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})})
              .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})})
              .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})})
              .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})})
          },
          close: () => Promise.resolve(closeCalled = true)
        })})}
      }))

      const stream = esp.subscribeToStream('mystream', {maxCount: 2})
      const events = await_(_(stream))
      assert.equal(events.length, 2)
    }))

    it('should close channel on subscription stream.close', aasync(() => {
      let closeCalled = false

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({connect: cb => cb(), query: () => {}})},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options)  => Promise.resolve(),
          assertQueue:    (queue, options)           => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange, pattern) => Promise.resolve(),
          consume:        (queue, cb, options)       => Promise.resolve(),
          close:          ()                         => Promise.resolve(closeCalled = true),
        })})}
      }))

      const stream = esp.subscribeToStream('mystream')
      aawait(new Promise((resolve, reject) => {stream.on('open', () => resolve())}))
      aawait(stream.close())

      assert(closeCalled)
    }))
  })

  describe('esp.readAllEventsForward()', () => {
    it('should get all already stored events', aasync(() => {
      const mysqlQuery = sinon.stub()
      const queryStream = new Stream
      mysqlQuery.returns(queryStream)

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          query:   mysqlQuery,
        })},
        amqpLib:  {connect: options => Promise.resolve()},
      }))

      const stream = esp.readAllEventsForward(0, {})
      process.nextTick(() => {
        queryStream.emit('result', {eventNumber: 5, streamId: 'foo'})
        queryStream.emit('result', {eventNumber: 2, streamId: 'bar'})
        queryStream.emit('result', {eventNumber: 8, streamId: 'baz'})
        queryStream.emit('end')
      })
      const events = await_(_(stream))

      assert.equal(events[0].streamId, 'foo')
      assert.equal(events[1].streamId, 'bar')
      assert.equal(events[2].streamId, 'baz')
    }))
  })

  describe('esp.readStreamEventsForward()', () => {
    it('should get all already stored events when starting at 0', aasync(() => {
      let mysqlValues = null

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          query:   (query, values) => {
            const stream = new Stream
            mysqlValues = values

            process.nextTick(() => {
              stream.emit('result', {eventNumber: 0})
              stream.emit('result', {eventNumber: 1})
              stream.emit('result', {eventNumber: 2})
              stream.emit('end')
            })
            return stream
          }
        })},
        amqpLib:  {connect: options => Promise.resolve()},
      }))

      const stream = esp.readStreamEventsForward('mystream', 0)
      const events = await_(_(stream))

      assert.equal(events.length, 3)
      assert.equal(events[2].eventNumber, 2)
      assert.equal(mysqlValues[0], 'mystream')
      assert.equal(mysqlValues[1], 0)
    }))
  })

  describe('esp.subscribeToAllFrom()', () => {
    it('should get existing events', (done) => {
      const esp = Object.create(Esp.EspPrototype)
      esp.readAllEventsForward = () => {
        const stream = new Stream
        process.nextTick(() => {stream.emit('data', {eventType: 'myTestEvent'})})
        return stream
      }
      esp.subscribeToStream = sinon.stub().returns(new Stream.Readable)

      const stream = esp.subscribeToAllFrom(0)
      stream.on('data', event => {
        assert(esp.subscribeToStream.calledWith('$all'))
        assert.equal(event.eventType, 'myTestEvent')
        done()
      })
    })

    it('should emit `headOfStream` when going from read to subscribe', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)
      esp.readAllEventsForward = () => {
        const stream = new Stream
        process.nextTick(() => {stream.emit('data', {eventType: 'myTestEvent'})})
        return stream
      }
      esp.subscribeToStream = sinon.stub().returns(new Stream.Readable)

      const stream = esp.subscribeToAllFrom(0)
      stream.on('data', event => {

        assert(esp.subscribeToStream.calledWith('$all'))
        assert.equal(event.eventType, 'myTestEvent')

      })
    }))
  })

  describe('esp.readStreamEventsUntil()', () => {
    it('should return only wanted events', aasync(() => {
      const mysqlCalls = []

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          query:   (query, values, cb) => {
            const sql = query.replace(/\?/g, () => values.shift())
            mysqlCalls.push(sql)

            if (sql.match(/^SELECT MAX/)) return cb(null, [{max: 100}], ['MAX'])

            //if (sql.match(/^SELECT \*/)) {
            const stream = new Stream
            process.nextTick(() => {
              stream.emit('result', {eventNumber: 5})
              stream.emit('end')
            })
            return stream
          }
        })},
        amqpLib:  {connect: options => Promise.resolve()},
      }))

      const stream = esp.readStreamEventsUntil('mystream', 5, 5)
      const events = await_(_(stream))

      // Note that the regex ?-replacement doesn't escape values.
      assert(mysqlCalls[1].match(/streamId = mystream/), 'should select from mystream')
      assert(mysqlCalls[2].match(/eventNumber >= 5/))
      assert(mysqlCalls[2].match(/LIMIT 1/))
      assert.equal(events.length, 1)
    }))

    it('should handle extra event from second mysql read without doubling it', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          query:   (query, values, cb) => {
            const sql = query.replace(/\?/g, () => values.shift())
            if (sql.match(/^SELECT MAX/)) return cb(null, [{max: 6}], ['MAX'])

            //if (sql.match(/^SELECT \*/)) {
            const stream = new Stream
            process.nextTick(() => {
              stream.emit('result', {eventNumber: 5})
              stream.emit('result', {eventNumber: 6})
              stream.emit('result', {eventNumber: 7})
              stream.emit('end')
            })
            return stream
          }
        })},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(),
          assertQueue:    (queue, options)  => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange) => Promise.resolve(),
          consume:        (queue, cb, options)  => {
            process.nextTick(() => cb({content: new Buffer(JSON.stringify({eventNumber: 7}))}))
            process.nextTick(() => cb({content: new Buffer(JSON.stringify({eventNumber: 8}))}))
            return Promise.resolve()
          },
          close:          () => Promise.resolve(),
        })})}
      }))

      const stream = esp.readStreamEventsUntil('mystream', 5, 8)
      const events = await_(_(stream))

      assert.equal(events.length, 4)
      assert.equal(events[0].eventNumber, 5)
      assert.equal(events[1].eventNumber, 6)
      assert.equal(events[2].eventNumber, 7)
      assert.equal(events[3].eventNumber, 8)
    }))

    it('should wait for coming events if not all exist', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          query:   (query, values, cb) => {
            const sql = query.replace(/\?/g, () => values.shift())
            if (sql.match(/^SELECT MAX/)) return cb(null, [{max: 6}], ['MAX'])

            //if (sql.match(/^SELECT \*/)) {
            const stream = new Stream
            process.nextTick(() => {
              stream.emit('result', {eventNumber: 5})
              stream.emit('result', {eventNumber: 6})
              stream.emit('end')
            })
            return stream
          }
        })},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(),
          assertQueue:    (queue, options)  => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange) => Promise.resolve(),
          consume:        (queue, cb, options)  => Promise.resolve(process.nextTick(() => cb({
            content: new Buffer(JSON.stringify({eventNumber: 7}))
          }))),
          close: () => Promise.resolve()
        })})}
      }))

      const stream = esp.readStreamEventsUntil('mystream', 5, 7)
      const events = await_(_(stream))

      assert.equal(events[0].eventNumber, 5)
      assert.equal(events[1].eventNumber, 6)
      assert.equal(events[2].eventNumber, 7)
    }))

    it('should respect timeout', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect: cb => cb(),
          query:   (query, values, cb) => {
            const sql = query.replace(/\?/g, () => values.shift())
            if (sql.match(/^SELECT MAX/)) return cb(null, [6], ['MAX'])

            //if (sql.match(/^SELECT \*/)) {
            const stream = new Stream
            process.nextTick(() => {
              stream.emit('end')
            })
            return stream
          }
        })},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(),
          assertQueue:    (queue, options)          => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange)         => Promise.resolve(),
          consume:        (queue, cb, options)      => Promise.resolve(),
          close:          ()                        => Promise.resolve()
        })})}
      }))

      const clock = sinon.useFakeTimers()
      const stream = esp.readStreamEventsUntil('mystream', 7, 8, {timeout: 9000})
      aawait(new Promise((resolve, reject) => {
        stream.on('error', () => resolve())
        clock.tick(9000)
      }))
    }))
  })

  describe('esp.writeEvents()', () => {
    it('should add events to mysql in a transaction and send them to rabbit', aasync(() => {
      const mysqlCalls     = []
      const amqpPublished  = []
      let exchangeAsserted = false

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect:          cb => cb(),
          beginTransaction: cb => cb(),
          commit:           cb => cb(),
          query:   (query, values, cb) => {
            const sql = query.replace(/\?/g, () => JSON.stringify(values.shift()))
            mysqlCalls.push(sql)

            if (sql.match(/^SELECT.*MAX/)) {
              return cb(null, [{
                globalPostion: null,
                eventNumber: null,
                date: '2016-04-14 14:35:17.402727'
              }])
            }
            cb(null, 'foo')
          }
        })},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(exchangeAsserted = true),
          bindQueue:      (queue, exchange)         => Promise.resolve(),
          close:          ()                        => Promise.resolve(),
          publish:        (exchange, routingKey, content) => {
            amqpPublished.push({
              exchange:   exchange,
              routingKey: routingKey,
              content:    content,
            })
            return true
          },
        })})}
      }))

      const written = aawait(esp.writeEvents('mystream', -1, false, [
        {eventId: esp.createGuid(), data: {foo: 'bar'}},
        {eventId: esp.createGuid(), data: {baz: 'qux'}},
      ]))

      assert.equal(written.result, 0)
      assert.equal(written.lastEventNumber, 1)
      assert(mysqlCalls[3].indexOf('2016-04-14 14:35:17.402727') > 0)
      assert(mysqlCalls[3].indexOf('{\\"baz\\":\\"qux\\"}') > 0)
      assert.equal(amqpPublished.length, 2)
      assert(exchangeAsserted, 'Exchange must be asserted')
      assert(amqpPublished[0].content instanceof Buffer)
    }))

    it('should handle existing event eventNumber 0', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({
          connect:          cb => cb(),
          beginTransaction: cb => cb(),
          commit:           cb => cb(),
          rollback:         cb => cb(),
          query:   (query, values, cb) => {
            const sql = query.replace(/\?/g, () => JSON.stringify(values.shift()))

            if (sql.match(/^SELECT.*MAX/)) {
              return cb(null, [{
                globalPosition: 42,
                eventNumber: 0,
                date: '2016-04-14 14:35:17.402727'
              }])
            }
            cb(null, 'foo')
          }
        })},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(),
          bindQueue:      (queue, exchange)         => Promise.resolve(),
          close:          ()                        => Promise.resolve(),
          publish:        (exchange, routingKey, content) => true,
        })})}
      }))

      const written = aawait(esp.writeEvents('mystream', 0, false, [
        {eventId: esp.createGuid(), data: {foo: 'bar'}},
        {eventId: esp.createGuid(), data: {baz: 'qux'}},
      ]))

      assert.equal(written.result, 0)
      assert.equal(written.lastEventNumber, 2)
    }))
  })

  describe('Some constants…', () => {
    it('should have ExpectedVersion', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {},
        mysqlLib: {createConnection: options => ({connect: cb => cb(), query: () => {}})},
        amqpLib:  {connect: options => Promise.resolve()}
      }))

      assert.equal(esp.ExpectedVersion.NoStream, -1)
      assert.equal(esp.ExpectedVersion.Any,      -2)
    }))
  })
})
