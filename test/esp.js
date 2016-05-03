'use strict'

const Stream  = require('stream')
const assert  = require('assert')
const aawait  = require('asyncawait/await')
const aasync  = require('asyncawait/async')
const uuid    = require('uuid')
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
        mysqlLib: {createPool: options => ({
          getConnection: cb => {
            assert.deepEqual(options, {dateStrings: true, my: 'mysql-options'})
            return cb(null, {release: () => {}})
          },
          fingerprint: 'my-mysql',
        })},
        amqpLib:  {connect: options => Promise.resolve('myAmqpConn')}
      }))

      assert.equal(esp.mysqlPool.fingerprint, 'my-mysql')
      assert.equal(esp.amqpConn, 'myAmqpConn')
    }))
  })

  describe('esp.close()', () => {
    it('should close connections', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)

      const mysqlPoolEnd = sinon.stub()
      mysqlPoolEnd.callsArg(0)
      esp.mysqlPool = {end: mysqlPoolEnd}

      const amqpClose = sinon.stub()
      amqpClose.returns(Promise.resolve());
      esp.amqpConn = {close: amqpClose}

      aawait(esp.close())

      assert(mysqlPoolEnd.calledOnce)
      assert(amqpClose.calledOnce)
    }))
  })

  describe('esp.createGuid()', () => {
    it('should give a string', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)

      assert.equal(typeof esp.createGuid(), 'string')
    }))
  })

  describe('esp.ping()', () => {
    it('should promise a pong', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)

      const mysqlPing = sinon.stub()
      mysqlPing.callsArg(0)
      esp.mysqlPool = {ping: mysqlPing}

      aawait(esp.ping())

      assert(mysqlPing.calledOnce)
    }))
  })

  describe('esp.subscribeToStream()', () => {
    it('should get new events when subscribed', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)

      esp.options = {amqp: {exchange: 'fooEvents'}}

      const assertExchange = sinon.stub().returns(Promise.resolve())
      const assertQueue = sinon.stub().returns(Promise.resolve({queue: 'foo'}))
      const bindQueue = sinon.stub().returns(Promise.resolve())
      let consumeQueue = null

      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: assertExchange,
          assertQueue:    assertQueue,
          bindQueue:      bindQueue,
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
        })
      }

      const event = aawait(new Promise((resolve, reject) => {
        const stream = esp.subscribeToStream('mystream')
        stream.on('data', event => resolve(event))
      }))

      assert(assertExchange.calledOnce)
      assert(assertExchange.calledWith('fooEvents', 'topic', {durable: true}))
      assert(assertQueue.calledOnce)
      assert(bindQueue.calledWith('foo', 'fooEvents', 'mystream'))
      assert.equal(event.streamId,  'mystream')
      assert.equal(event.eventType, 'myEventType')
      assert.deepEqual(event.data,  {my: 'eventdata'}, 'Event data should be parsed')
    }))

    it('should close on maxCount', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)
      esp.options = {amqp: {exchange: 'fooEvents'}}

      let closeCalled = false

      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: () => Promise.resolve(),
          assertQueue:    () => Promise.resolve({queue: 'foo'}),
          bindQueue:      () => Promise.resolve(),
          consume:        (queue, cb, options) => Promise.resolve()
            .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})})
            .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})})
            .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})})
            .then(() => {if (!closeCalled) cb({content: new Buffer(JSON.stringify({}))})}),
          close: () => Promise.resolve(closeCalled = true)
        })
      }

      const stream = esp.subscribeToStream('mystream', {maxCount: 2})
      const events = await_(_(stream))
      assert.equal(events.length, 2)
    }))

    it('should close channel on subscription stream.close', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)
      esp.options = {amqp: {exchange: 'fooEvents'}}

      const close = sinon.stub().returns(Promise.resolve())

      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: () => Promise.resolve(),
          assertQueue:    () => Promise.resolve({queue: 'foo'}),
          bindQueue:      () => Promise.resolve(),
          consume:        () => Promise.resolve(),
          close:          close,
        })
      }

      const stream = esp.subscribeToStream('mystream')
      aawait(new Promise((resolve, reject) => {stream.on('open', () => resolve())}))
      aawait(stream.close())

      assert(close.calledOnce)
    }))
  })

  describe('esp.readAllEventsForward()', () => {
    it('should get all already stored events', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)

      const myUuidBuffer = new Buffer(32)
      const myUuid       = uuid.v4({}, myUuidBuffer)
      const queryStream  = new Stream
      const mysqlQuery   = sinon.stub().returns(queryStream)

      esp.mysqlPool = {query: mysqlQuery}

      const stream = esp.readAllEventsForward(0, {})
      process.nextTick(() => {
        queryStream.emit('result', {eventId: myUuid, eventNumber: 5, streamId: 'foo'})
        queryStream.emit('result', {eventId: myUuid, eventNumber: 2, streamId: 'bar'})
        queryStream.emit('result', {eventId: myUuid, eventNumber: 8, streamId: 'baz'})
        queryStream.emit('end')
      })
      const events = await_(_(stream))

      assert.equal(events[0].eventId, uuid.unparse(myUuid))
      assert.equal(events[0].streamId, 'foo')
      assert.equal(events[1].streamId, 'bar')
      assert.equal(events[2].streamId, 'baz')
    }))
  })

  describe('esp.readStreamEventsForward()', () => {
    it('should get all already stored events when starting at 0', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)

      let mysqlValues = null

      esp.mysqlPool = {
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
      }

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
        assert(esp.subscribeToStream.calledWith('#'))
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
      const esp = Object.create(Esp.EspPrototype)

      const mysqlCalls = []

      esp.mysqlPool = {
        query:   (query, values, cb) => {
          const sql = query.replace(/\?/g, () => values.shift())
          mysqlCalls.push(sql)

          if (sql.match(/^SELECT MAX/)) return cb(null, [{max: 100}], ['MAX'])

          const stream = new Stream
          process.nextTick(() => {
            stream.emit('result', {eventNumber: 5})
            stream.emit('end')
          })
          return stream
        }
      }

      const stream = esp.readStreamEventsUntil('mystream', 5, 5)
      const events = await_(_(stream))

      // Note that the regex ?-replacement doesn't escape values.
      assert(mysqlCalls[0].match(/streamId = mystream/), 'should select from mystream')
      assert(mysqlCalls[1].match(/eventNumber >= 5/))
      assert(mysqlCalls[1].match(/LIMIT 1/))
      assert.equal(events.length, 1)
    }))

    it('should handle extra event from second mysql read without doubling', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)
      esp.options = {amqp: {exchange: 'fooEvents'}}

      esp.mysqlPool = {
        query:   (query, values, cb) => {
          const sql = query.replace(/\?/g, () => values.shift())
          if (sql.match(/^SELECT MAX/)) return cb(null, [{max: 6}], ['MAX'])

          const stream = new Stream
          process.nextTick(() => {
            stream.emit('result', {eventNumber: 5})
            stream.emit('result', {eventNumber: 6})
            stream.emit('result', {eventNumber: 7})
            stream.emit('end')
          })
          return stream
        }
      }
      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: () => Promise.resolve(),
          assertQueue:    () => Promise.resolve({queue: 'foo'}),
          bindQueue:      () => Promise.resolve(),
          consume:        (queue, cb, options)  => {
            process.nextTick(() => cb({content: new Buffer(JSON.stringify({eventNumber: 7}))}))
            process.nextTick(() => cb({content: new Buffer(JSON.stringify({eventNumber: 8}))}))
            return Promise.resolve()
          },
          close:          () => Promise.resolve(),
        })
      }

      const stream = esp.readStreamEventsUntil('mystream', 5, 8)
      const events = await_(_(stream))

      assert.equal(events.length, 4)
      assert.equal(events[0].eventNumber, 5)
      assert.equal(events[1].eventNumber, 6)
      assert.equal(events[2].eventNumber, 7)
      assert.equal(events[3].eventNumber, 8)
    }))

    it('should wait for coming events if not all exist', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)
      esp.options = {amqp: {exchange: 'fooEvents'}}

      esp.mysqlPool = {
        query:   (query, values, cb) => {
          const sql = query.replace(/\?/g, () => values.shift())
          if (sql.match(/^SELECT MAX/)) return cb(null, [{max: 6}], ['MAX'])

          const stream = new Stream
          process.nextTick(() => {
            stream.emit('result', {eventNumber: 5})
            stream.emit('result', {eventNumber: 6})
            stream.emit('end')
          })
          return stream
        }
      }
      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(),
          assertQueue:    (queue, options)  => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange) => Promise.resolve(),
          consume:        (queue, cb, options) => {
            process.nextTick(() => cb({
              content: new Buffer(JSON.stringify({eventNumber: 7}))
            }))
            return Promise.resolve()
          },
          close: () => Promise.resolve()
        })
      }

      const stream = esp.readStreamEventsUntil('mystream', 5, 7)
      const events = await_(_(stream))

      assert.equal(events[0].eventNumber, 5)
      assert.equal(events[1].eventNumber, 6)
      assert.equal(events[2].eventNumber, 7)
    }))

    it('should respect timeout', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)
      esp.options = {amqp: {exchange: 'fooEvents'}}

      esp.mysqlPool = {
        query:   (query, values, cb) => {
          const sql = query.replace(/\?/g, () => values.shift())
          if (sql.match(/^SELECT MAX/)) return cb(null, [6], ['MAX'])

          const stream = new Stream
          process.nextTick(() => {
            stream.emit('end')
          })
          return stream
        }
      }
      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: (exchange, type, options) => Promise.resolve(),
          assertQueue:    (queue, options)          => Promise.resolve({queue: 'foo'}),
          bindQueue:      (queue, exchange)         => Promise.resolve(),
          consume:        (queue, cb, options)      => Promise.resolve(),
          close:          ()                        => Promise.resolve()
        })
      }

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
      const esp = Object.create(Esp.EspPrototype)
      esp.options = {amqp: {exchange: 'fooEvents'}}

      const mysqlCalls     = []
      const amqpPublished  = []

      const beginTransaction = sinon.stub().callsArg(0)
      const commit           = sinon.stub().callsArg(0)
      const release          = sinon.spy()
      const assertExchange   = sinon.stub().returns(Promise.resolve())

      esp.mysqlPool = {
        getConnection: cb => {cb(null, {
          beginTransaction: beginTransaction,
          commit:           commit,
          query:   (query, values, cb) => {
            const sql = query.replace(/\?/g, () => JSON.stringify(values.shift()))
            mysqlCalls.push(sql)

            if (sql.match(/^SELECT.*MAX/)) {
              return cb(null, [{
                globalPosition: null,
                eventNumber:    null,
                date:           '2016-04-14 14:35:17.402727'
              }])
            }
            cb(null, 'foo')
          },
          release: release,
        })}
      }
      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: assertExchange,
          bindQueue:      () => Promise.resolve(),
          close:          () => Promise.resolve(),
          publish:        (exchange, routingKey, content) => {
            amqpPublished.push({
              exchange:   exchange,
              routingKey: routingKey,
              content:    content,
            })
            return true
          },
        })
      }

      const written = aawait(esp.writeEvents('mystream', -1, false, [
        {eventId: esp.createGuid(), data: {foo: 'bar'}},
        {eventId: esp.createGuid(), data: {baz: 'qux'}},
      ]))

      assert.equal(written.result, 0)
      assert.equal(written.firstEventNumber, 0)
      assert.equal(written.lastEventNumber, 1)
      assert(mysqlCalls[2].indexOf('2016-04-14 14:35:17.402727') > 0)
      assert(mysqlCalls[2].indexOf('{\\"baz\\":\\"qux\\"}') > 0)
      assert.equal(amqpPublished.length, 2)
      assert(assertExchange.calledOnce)
      assert(amqpPublished[0].content instanceof Buffer)
      assert.equal(JSON.parse(amqpPublished[0].content).globalPosition, 0)
      assert.equal(JSON.parse(amqpPublished[1].content).globalPosition, 1)
      assert(release.calledOnce)
    }))

    it('should handle existing event eventNumber 0', aasync(() => {
      const esp = Object.create(Esp.EspPrototype)
      esp.options = {amqp: {exchange: 'fooEvents'}}

      esp.mysqlPool = {
        getConnection: cb => {cb(null, {
          beginTransaction: cb => cb(),
          commit:           cb => cb(),
          rollback:         cb => cb(),
          release:          () => {},
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
        })}
      }
      esp.amqpConn = {
        createChannel: () => Promise.resolve({
          assertExchange: () => Promise.resolve(),
          bindQueue:      () => Promise.resolve(),
          close:          () => Promise.resolve(),
          publish:        () => true,
        })
      }

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
      const esp = Object.create(Esp.EspPrototype)

      assert.equal(esp.ExpectedVersion.NoStream, -1)
      assert.equal(esp.ExpectedVersion.Any,      -2)
    }))
  })
})
