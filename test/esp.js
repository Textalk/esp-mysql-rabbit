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
            return Promise.resolve(
              setTimeout(() => cb({
                content: new Buffer(JSON.stringify({
                  streamId:  'mystream',
                  eventType: 'myEventType',
                  updated:   '1970-01-01T12:13:42.492473Z',
                  data:      {my: 'eventdata'},
                }))
              }), 1)
            )
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

      console.log('foo?', mysqlCalls[0].match(/eventNumber >= 5/))
      assert(mysqlCalls[0].match(/streamId = 'mystream'/))
      assert(mysqlCalls[0].match(/eventNumber >= 5/))
      assert(mysqlCalls[0].match(/LIMIT 1/))
    }))
  })
})
