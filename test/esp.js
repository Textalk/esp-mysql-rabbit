'use strict'

const Stream = require('stream')
const assert = require('assert')
const aawait = require('asyncawait/await')
const aasync = require('asyncawait/async')
const sinon  = require('sinon')
const Esp    = require('../esp')
const _      = require('highland')
const await_ = stream => aawait(new Promise((res, rej) => stream.errors(rej).toArray(res)))

describe('esp mysql rabbit', () => {
  describe('Esp factory', () => {
    it('should connect to mysql and amqp, returning esp', aasync(() => {
      const esp = aawait(Esp({
        mysql:    {my: 'mysql-options'},
        amqp:     {my: 'amqp-options', exchange: 'events'},
        mysqlLib: {createConnection: options => {
          assert.deepEqual(options, {my: 'mysql-options'})
          return {fingerprint: 'my-mysql', connect: (cb) => cb()}
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
        mysqlLib: {createConnection: options => ({connect: cb => cb()})},
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

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({connect: cb => cb()})},
        amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(assertExchangeCalled = true),
          assertQueue:    (queue, options)  => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange) => Promise.resolve('ok'),
          consume:        (queue, cb, options)  => {
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
      assert.equal(event.streamId,  'mystream')
      assert.equal(event.eventType, 'myEventType')
      assert.deepEqual(event.data,  {my: 'eventdata'}, 'Event data should be parsed')
    }))

    it('should close on maxCount', aasync(() => {
      let closeCalled = false

      const esp = aawait(Esp({
        mysql:    {},
        amqp:     {exchange: 'events'},
        mysqlLib: {createConnection: options => ({connect: cb => cb()})},
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
        mysqlLib: {createConnection: options => ({connect: cb => cb()})},
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

            if (sql.match(/^SELECT MAX/)) return cb(null, [100], ['MAX'])

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
      assert(mysqlCalls[0].match(/streamId = mystream/), 'should select from mystream')
      assert(mysqlCalls[1].match(/eventNumber >= 5/))
      assert(mysqlCalls[1].match(/LIMIT 1/))
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
            if (sql.match(/^SELECT MAX/)) return cb(null, [6], ['MAX'])

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
            if (sql.match(/^SELECT MAX/)) return cb(null, [6], ['MAX'])

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
})