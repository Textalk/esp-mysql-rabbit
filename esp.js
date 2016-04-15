'use strict'

const Readable = require('stream').Readable
const util     = require('util')
const mysql    = require('mysql2')
const amqp     = require('amqplib')
const uuid     = require('uuid')
const Promise  = require('bluebird') // At least until we have array destructuring!

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
      const mysqlOptions = Object.assign({}, {dateStrings: true}, options.mysql)
      const mysqlConn    = esp.mysqlLib.createConnection(mysqlOptions)

      // Explicitly connect, to find out any connection errors directly.
      return Promise.all([
        new Promise((resolve, reject) => mysqlConn.connect(
          err => (err ? reject(err) : resolve(mysqlConn))
        )),
        esp.amqpLib.connect(options.amqp.url),
      ])
    })
    .spread((mysqlConn, amqpConn) => {
      esp.mysqlConn = mysqlConn
      esp.amqpConn  = amqpConn

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
      channel.assertExchange(this.options.amqp.exchange, 'topic', {durable: true}),
      channel.assertQueue(null, {durable: false, exclusive: true, autoDelete: true}),
    ]))
    .spread((channel, ok, queueResult) => {
      // Setup the close method, now that we have all variables needed.
      stream.close = () => channel.close()

      return Promise.all([
        channel, queueResult.queue,
        channel.bindQueue(queueResult.queue, this.options.amqp.exchange, streamId)
      ])
    })
    .spread((channel, queue, ok) => {
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
    clearTimeout(timer)
  }

  Promise.resolve()
    // Check last eventNumber in MySQL.
    .then(() => new Promise((resolve, reject) => this.mysqlConn.query(
      'SELECT MAX(eventNumber) AS max FROM events WHERE streamId = ?', [streamId],
      (err, result, fields) => (err ? reject(err) : resolve(result[0]))
    )))
    .then(result => {
      const max = result.max
      const subStream = (max >= to) ? null
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

      subStream.on('data', event => {
        if (event.eventNumber > lastEventNumber) stream.push(event)
      })
      subStream.on('end', ()     => end())
      subStream.resume()
    })
    .catch(err => stream.emit('error', err))

  return stream
}

const uuid2Binary = uuid => {
  return new Buffer(uuid.replace(/\-/g, ''), 'hex')
}

EspPrototype.writeEvents = function(streamId, expectedVersion, requireMaster, events) {
  return Promise.resolve()
    // Start transaction.
    .then(() => new Promise((resolve, reject) => this.mysqlConn.beginTransaction(
      err => (err ? reject(err) : resolve())
    )))

    // Lock stream rows.
    .then(() => new Promise((resolve, reject) => this.mysqlConn.query(
      'SELECT MAX(eventNumber) AS max, UTC_TIMESTAMP(6) AS date '
        + 'FROM events WHERE streamId = ? LOCK IN SHARE MODE', [streamId],
      (err, result) => (err ? reject(err) : resolve(result[0]))
    )))

    // Format data and insert to MySQL.
    .then(result => {
      let   max     = result.max
      const updated = result.date

      if (
        (expectedVersion === this.ExpectedVersion.NoStream && max !== null)
          || (expectedVersion >= 0 && max !== expectedVersion)
      ) throw new Error('Unexpected version')

      if (max === null) max = -1

      // Assign eventNumber to all events.
      events.forEach(event => {
        event.eventNumber = ++max
      })

      const eventsData = events.map(event => [
        streamId,
        event.eventNumber,
        uuid2Binary(event.eventId),
        event.eventType,
        updated,
        JSON.stringify(event.data)
      ])

      return Promise.all([
        eventsData, max,
        new Promise((resolve, reject) => this.mysqlConn.query(
          'INSERT INTO events (streamId, eventNumber, eventId, eventType, updated, data) VALUES ?',
          [eventsData],
          (err, result) => (err ? reject(err) : resolve(result))
        ))
      ])
    })

    // MySQL commit
    .spread((eventsData, max, ok) => Promise.all([
      eventsData, max,
      new Promise((resolve, reject) => this.mysqlConn.commit(
        err => (err ? reject(err) : resolve())
      ))
    ]))

    // Catch MySQL problems to rollback and throw
    .catch(err => {
      return new Promise((resolve, reject) => this.mysqlConn.rollback(() => reject(err)))
    })

    // Create AMQP Channel
    .spread((eventsData, max, ok) => Promise.all([
      eventsData, max, this.amqpConn.createChannel()
    ]))

    // Assert events Exchanage
    .spread((eventsData, max, channel) => Promise.all([
      eventsData, max, channel,
      channel.assertExchange(this.options.amqp.exchange, 'topic', {durable: true}),
    ]))

    // Publish to AMQP
    .spread((eventsData, max, channel, ok) => {
      const published = events.map(event => channel.publish(
        this.options.amqp.exchange, streamId, new Buffer(JSON.stringify(event))
      ))

      // If any publish-call returned false, we're in trouble…
      const publishedOk = published.reduce((prev, curr) => (prev & curr), true)

      return {
        result:           0,
        firstEventNumber: eventsData[0].eventNumber,
        lastEventNumber:  max,
        publishedOk:      publishedOk,
      }
    })
}

EspPrototype.ExpectedVersion = {
  Any: -2,
  NoStream: -1,
}

module.exports = Esp
