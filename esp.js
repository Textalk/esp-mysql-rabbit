'use strict'

const Readable = require('stream').Readable
const util     = require('util')
const mysql    = require('mysql2')
const amqp     = require('amqplib')
const uuid     = require('uuid')

const EspPrototype = {}

const EventStoreStream = function() {Readable.call(this, {objectMode: true})}
util.inherits(EventStoreStream, Readable)
EventStoreStream.prototype._read = (size) => {}

const defaultDefaults = {
  timeout:        2000,
  resolveLinkTos: false,
  requireMaster:  false,
  maxCount:       4095,
}

const Esp = (options, defaults) => {
  const esp = Object.create(EspPrototype)

  // Allow switching of mysql-lib (for chosing mysql or mysql2, or mocking).
  esp.mysqlLib = options.mysqlLib || mysql
  esp.amqpLib  = options.amqpLib  || amqp

  esp.options  = options
  esp.defaults = Object.assign({}, defaultDefaults, defaults)

  return Promise.resolve()
    // Connect to MySQL and Amqp
    .then(() => {
      const mysqlConn = esp.mysqlLib.createConnection(options.mysql)

      // Explicitly connect, to find out any connection errors directly.
      return Promise.all([
        new Promise((resolve, reject) => mysqlConn.connect(
          err => (err ? reject(err) : resolve(mysqlConn))
        )),
        esp.amqpLib.connect(options.amqp),
      ])
    })
    .then(result => {
      esp.mysqlConn = result[0]
      esp.amqpConn  = result[1]

      /// @todo Catch unexpected close and errors.
      // var dom = domain.create();
      // dom.on('error', gracefullyRestart);
      // amqp.connect(function(err, conn) …

      return esp
    })
}

EspPrototype.close = function() {
  return Promise.all([
    new Promise(
      (resolve, reject) => this.mysqlConn.end(err => (err ? reject(err) : resolve()))
    ),
    this.amqpConn.close()
  ])
}

EspPrototype.createGuid = uuid.v4

EspPrototype.ping = function() {
  // Will only ping MySQL now.
  return new Promise(
    (resolve, reject) => this.mysqlConn.ping(err => (err ? reject(err) : resolve()))
  )
}

EspPrototype.subscribeToStream = function(streamId, params) {
  const options = Object.assign({}, this.defaults, params)
  const stream  = new EventStoreStream()

  this.amqpConn.createChannel()
    .then(channel => Promise.all([
      channel,
      channel.assertExchange(this.options.amqp.exchange, 'fanout', {durable: true}),
      channel.assertQueue(null, {durable: false, exclusive: true, autoDelete: true}),
    ]))
    .then(result => {
      const channel = result[0]
      const queue   = result[2].queue

      // Setup the close method, now that we have all variables needed.
      stream.close = () => channel.close()

      return Promise.all([
        channel, queue, channel.bindQueue(queue, this.options.amqp.exchange, streamId)
      ])
    })
    .then(result => {
      const channel = result[0]
      const queue   = result[1]
      let eventCount = 0

      return channel.consume(queue, msg => {
        stream.push(JSON.parse(msg.content))
        if (++eventCount >= options.maxCount) {
          stream.push(null)
          channel.close()
        }
      })
    })
    .then(ok => stream.emit('open'))
    .catch(err => stream.emit('error', err))

  return stream
}

const mysqlToEvent = row => ({
  eventId:     row.eventId,    // todo Convert to string from varbin uuid
  eventType:   row.eventType,
  eventNumber: row.eventNumber,
  data:        row.data ? JSON.parse(row.data) : null,
  streamId:    row.streamId,
  // EventStore uses a lot of meta fields, isJson, isMetaData, author etc…
  updated:     row.updated,
})

EspPrototype.readStreamEventsForward = function(streamId, from, params) {
  const options = Object.assign({}, this.defaults, params)
  const stream  = new EventStoreStream()

  const query = this.mysqlConn.query(
    'SELECT * FROM events WHERE streamId = ? AND eventNumber >= ? ' +
      'ORDER BY eventNumber LIMIT ?',
    [streamId, from, options.maxCount]
  )
  query
    .on('error', err => stream.emit('error', err))
    .on('end',   ()  => stream.push(null))
    //.on('fields', fields =>
    .on('result', row => stream.push(mysqlToEvent(row)))

  return stream
}

EspPrototype.readStreamEventsUntil = function(streamId, from, to, params) {
  const options = Object.assign({}, this.defaults, params, {maxCount: to - from + 1})
  const stream  = new EventStoreStream()
  const timer   = setTimeout(
    () => stream.emit('error', new Error('Timeout reached')),
    options.timeout
  )

  const end = () => {
    stream.push(null)
    return clearTimeout(timer)
  }

  Promise.resolve()
    // Check last eventNumber in MySQL.
    .then(() => new Promise((resolve, reject) => this.mysqlConn.query(
      'SELECT MAX(eventNumber) FROM events WHERE streamId = ?', [streamId],
      (err, result, fields) => (err ? reject(err) : resolve(result[0]))
    )))
    .then(max => {
      const subStream = (max > to) ? null
            : this.subscribeToStream(streamId, Object.assign({}, options, {maxCount: to - max}))
      if (subStream) subStream.on('error', err => stream.emit('error', err))
      if (subStream) subStream.pause()

      let lastEventNumber = -1
      return Promise.all([
        subStream,
        new Promise((resolve, reject) => this.readStreamEventsForward(streamId, from, options)
          .on('error', err   => reject(err))
          .on('end',   ()    => resolve(lastEventNumber))
          .on('data',  event => {
            stream.push(event)
            lastEventNumber = event.eventNumber
          })
        )
      ])
    })
    .then(result => {
      const subStream       = result[0]
      const lastEventNumber = result[1]
      if (lastEventNumber === to) return end()

      subStream.on('data', event => {if (event.eventNumber > lastEventNumber) stream.push(event)})
      subStream.on('end', ()     => end())
      subStream.resume()
    })
    .catch(err => stream.emit('error', err))

  return stream
}

module.exports = Esp
