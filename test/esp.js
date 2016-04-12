'use strict'

const assert = require('assert')
const aawait = require('asyncawait/await')
const aasync = require('asyncawait/async')
const sinon  = require('sinon')
const Esp    = require('../esp')

describe('esp', () => {
  it('should connect to mysql and amqp, returning esp', aasync(() => {
    //const assertExchange = sinon.spy()
    //const amqpChannel = {assertExchange: assertExchange}
    //assert.equal(assertExchange.callCount, 1)
    //assert(assertExchange.calledWith('events', 'fanout', {durable: true}))

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

  it('should close connections on close', aasync(() => {
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

  it('should give a string on createGuid', aasync(() => {
    const esp = aawait(Esp({
      mysql:    {},
      amqp:     {exchange: 'events'},
      mysqlLib: {createConnection: options => ({connect: cb => cb()})},
      amqpLib:  {connect: options => Promise.resolve()},
    }))

    assert.equal(typeof esp.createGuid(), 'string')
  }))

  it('should promise a pong on ping', aasync(() => {
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

  it('should get new events when subscribed', aasync(() => {
    const esp = aawait(Esp({
      mysql:    {},
      amqp:     {exchange: 'events'},
      mysqlLib: {createConnection: options => ({connect: cb => cb()})},
      amqpLib:  {connect: options => Promise.resolve({createChannel: () => Promise.resolve({
        assertExchange: (exchange, type, options) => Promise.resolve(),
        assertQueue:    (queue, options)  => Promise.resolve({queue: 'foo'}),
        bindQueue:      (queue, exchange) => Promise.resolve('ok'),
        consume:        (queue, cb, options)  => Promise.resolve(
          setTimeout(() => cb({
            content: new Buffer(JSON.stringify({
              streamId: 'mystream',
              eventType: 'myEventType',
              updated:   '1970-01-01T12:13:42.492473Z',
              data:      {my: 'eventdata'},
            }))
          }), 1)
        )
      })})}
    }))

    const event = aawait(new Promise((resolve, reject) => {
      const stream = esp.subscribeToStream('mystream')
      stream.on('data', event => resolve(event))
    }))

    assert.equal(event.streamId,  'mystream')
    assert.equal(event.eventType, 'myEventType')
    assert.deepEqual(event.data,      {my: 'eventdata'}, 'Event data should be parsed')
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
