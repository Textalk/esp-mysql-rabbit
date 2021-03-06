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

const defaultOptions = {
  eventsTable: 'events',
}

const Esp = (options, defaults) => {
  const esp = Object.create(EspPrototype)

  // Allow switching of mysql-lib (for chosing mysql or mysql2, or mocking).
  const mysqlLib = options.mysqlLib || mysql
  const amqpLib  = options.amqpLib  || amqp

  esp.options  = Object.assign({}, defaultOptions,  options)
  esp.defaults = Object.assign({}, defaultDefaults, defaults)

  return Promise.resolve()
    // Connect to MySQL and Amqp
    .then(() => {
      const mysqlOptions = Object.assign({}, {dateStrings: true}, options.mysql)
      esp.mysqlPool = mysqlLib.createPool(mysqlOptions)

      // Explicitly connect, to find out any connection errors directly.
      return Promise.all([
        new Promise((resolve, reject) => esp.mysqlPool.getConnection(
          (err, conn) => (err ? reject(err) : resolve(conn))
        )),
        amqpLib.connect(options.amqp.url),
      ])
    })
    .spread((mysqlConn, amqpConn) => {
      // We don't need mysqlConn, it's just to try out connection parameters.
      mysqlConn.release()
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
      (resolve, reject) => this.mysqlPool.end(err => (err ? reject(err) : resolve()))
    ),
    this.amqpConn.close()
  ])
}

EspPrototype.createGuid = uuid.v4

EspPrototype.ping = function() {
  // Will only ping MySQL now.
  return new Promise(
    (resolve, reject) => this.mysqlPool.ping(err => (err ? reject(err) : resolve()))
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
  globalPosition: row.globalPosition,
  eventId:        row.eventId ? uuid.unparse(row.eventId) : '',
  eventType:      row.eventType,
  eventNumber:    row.eventNumber,
  data:           row.data ? JSON.parse(row.data) : null,
  streamId:       row.streamId,
  // EventStore uses a lot of meta fields, isJson, isMetaData, author etc…
  updated:        row.updated,
})

EspPrototype.readAllEventsForward = function(from, params) {
  const options = Object.assign({}, this.defaults, params)
  const stream  = new EventStoreStream()

  this.mysqlPool.getConnection((err, connection) => {
    if (err) return stream.emit('error', err)

    const query = connection.query(
      `SELECT * FROM ${this.options.eventsTable} WHERE globalPosition >= ? ` +
        'ORDER BY globalPosition LIMIT ?',
      [from, options.maxCount]
    )
    query
      .on('error',  err => stream.emit('error', err))
      .on('end',    ()  => stream.push(null))
      .on('result', row => stream.push(mysqlToEvent(row)))
  })

  return stream
}

EspPrototype.readStreamEventsForward = function(streamId, from, params) {
  const options = Object.assign({}, this.defaults, params)
  const stream  = new EventStoreStream()

  this.mysqlPool.getConnection((err, connection) => {
    if (err) return stream.emit('error', err)

    const query = connection.query(
      `SELECT * FROM ${this.options.eventsTable} ` +
        'WHERE streamId = ? AND eventNumber >= ? ' +
        'ORDER BY eventNumber LIMIT ?',
      [streamId, from, options.maxCount]
    )
    query
      .on('error',  err => stream.emit('error', err))
      .on('end',    ()  => stream.push(null))
      .on('result', row => stream.push(mysqlToEvent(row)))
    connection.release()
  })

  return stream
}

EspPrototype.subscribeToStreamFrom = function(streamId, from, params) {
  const options = Object.assign({}, this.defaults, params)
  const stream  = new EventStoreStream()

  Promise.resolve()
    .then(() => {
      const subStream = this.subscribeToStream(streamId, params)
      subStream.on('error', err => stream.emit('error', err))
      subStream.pause()
      stream.close = subStream.close

      let eventNumber = -1
      return Promise.all([
        subStream,
        new Promise((resolve, reject) => this.readStreamEventsForward(streamId, from, options)
          .on('error', err   => reject(err))
          .on('end',   ()    => resolve(eventNumber))
          .on('data',  event => {
            stream.push(event)
            eventNumber = event.eventNumber
          })
        )
      ])
    })
    .then(result => {
      const subStream   = result[0]
      const eventNumber = result[1]

      subStream.on('data', event => {
        if (event.eventNumber > eventNumber) stream.push(event)
      })
      subStream.on('end', () => end())
      subStream.resume()
    })
    .catch(err => stream.emit('error', err))

  return stream
}

EspPrototype.subscribeToAllFrom = function(position, params) {
  const options = Object.assign({}, this.defaults, params)
  const stream  = new EventStoreStream()

  Promise.resolve()
    .then(() => {
      const subStream = this.subscribeToStream('#', params)
      subStream.on('error', err => stream.emit('error', err))
      subStream.pause()
      stream.close = subStream.close

      let globalPosition = -1
      return Promise.all([
        subStream,
        new Promise((resolve, reject) => this.readAllEventsForward(position, options)
          .on('error', err   => reject(err))
          .on('end',   ()    => resolve(globalPosition))
          .on('data',  event => {
            stream.push(event)
            globalPosition = event.globalPosition
          })
        )
      ])
    })
    .then(result => {
      const subStream      = result[0]
      const globalPosition = result[1]

      subStream.on('data', event => {
        if (event.globalPosition > globalPosition) stream.push(event)
      })
      subStream.on('end', () => end())
      subStream.resume()
    })
    .catch(err => stream.emit('error', err))

  return stream
}

EspPrototype.readStreamEventsUntil = function(streamId, from, to, params) {
  const options = Object.assign({}, this.defaults, params, {maxCount: to - from + 1})
  const stream  = new EventStoreStream()
  const timer   = setTimeout(
    () => stream.emit('error', new TimeoutError('Timeout reached')),
    options.timeout
  )

  const end = () => {
    stream.push(null)
    clearTimeout(timer)
  }

  Promise.resolve()
    // Check last eventNumber in MySQL.
    .then(() => new Promise((resolve, reject) => {
      this.mysqlPool.getConnection((err, connection) => {
        if (err) return reject(err)
        connection.query(
          `SELECT MAX(eventNumber) AS max FROM ${this.options.eventsTable} ` +
            'WHERE streamId = ?',
          [streamId],
          (err, result, fields) => {
            connection.release()
            if (err) return reject(err)
            resolve(result[0])
          }
        )
      })
    }))
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
      subStream.on('end', () => end())
      subStream.resume()
    })
    .catch(err => stream.emit('error', err))

  return stream
}

const uuid2Binary = uuid => {
  return new Buffer(uuid.replace(/\-/g, ''), 'hex')
}

let debugCounter = 0

EspPrototype.writeEvents = function(streamId, expectedVersion, requireMaster, events) {
  let mysqlConn = null // We need to have the connection even for errors.

  return Promise.resolve()
    // Get specific connection from pool.
    .then(() => new Promise((resolve, reject) => this.mysqlPool.getConnection(
      (err, connection) => (err ? reject(err) : resolve(connection))
    )))

    // Start transaction.
    .then(connection => {
      mysqlConn = connection

      return new Promise((resolve, reject) => connection.query(
        'SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ', [],
        err => {
          if (err) return reject(err)
          mysqlConn.beginTransaction(err => (err ? reject(err) : resolve()))
        }
      ))
    })

    // Make a global lock by agreement (since next-key-locking can produce deadlocks).
    .then(() => new Promise((resolve, reject) => mysqlConn.query(
      `SELECT 1 FROM ${this.options.eventsTable} WHERE globalPosition = 0 ` +
        'FOR UPDATE', [],
      (err, result) => (err ? reject(err) : resolve())
    )))

    // Lock stream rows.
    .then(() => Promise.all([
      new Promise((resolve, reject) => mysqlConn.query(
        'SELECT '
          + '  MAX(globalPosition) as globalPosition, '
          + '  UTC_TIMESTAMP(6) AS date '
          + `FROM ${this.options.eventsTable}`, [],
        (err, result) => (err ? reject(err) : resolve(result[0]))
      )),
      new Promise((resolve, reject) => mysqlConn.query(
        'SELECT MAX(eventNumber) AS eventNumber '
          + `FROM ${this.options.eventsTable} WHERE streamId = ?`, [streamId],
        (err, result) => (err ? reject(err) : resolve(result[0].eventNumber))
      )),
    ]))

    // Format data and insert to MySQL.
    .spread((result, eventNumber) => {
      let globalPosition = result.globalPosition
      const updated      = result.date

      if (
        (expectedVersion === this.ExpectedVersion.NoStream && eventNumber !== null)
          || (expectedVersion >= 0 && eventNumber !== expectedVersion)
      ) throw new Error('Unexpected version ' + expectedVersion + ' != ' + eventNumber)

      if (eventNumber    === null) eventNumber    = -1
      if (globalPosition === null) globalPosition = -1

      // Assign eventNumber to all events.
      events.forEach(event => {
        event.globalPosition = ++globalPosition
        event.eventNumber    = ++eventNumber
        event.updated        = updated
        event.streamId       = streamId
      })

      const eventsData = events.map(event => [
        event.globalPosition,
        event.streamId,
        event.eventNumber,
        uuid2Binary(event.eventId),
        event.eventType,
        event.updated,
        JSON.stringify(event.data)
      ])

      return Promise.all([
        eventNumber,
        new Promise((resolve, reject) => mysqlConn.query(
          `INSERT INTO ${this.options.eventsTable} `
            + '(globalPosition, streamId, eventNumber, eventId, eventType, updated, data) '
            + 'VALUES ?',
          [eventsData],
          (err, result) => (err ? reject(err) : resolve(result))
        ))
      ])
    })

    // MySQL commit.
    .spread((lastEventNumber, ok) => Promise.all([
      lastEventNumber,
      new Promise((resolve, reject) => mysqlConn.commit(
        err => {
          if (err) return reject(err)
          mysqlConn.release()
          mysqlConn = null
          resolve()
        }
      ))
    ]))

    // Catch MySQL problems to rollback and throw.
    .catch(err => new Promise((resolve, reject) => mysqlConn.rollback(() => {
      mysqlConn.release()
      mysqlConn = null
      reject(err)
    })))

    // Create AMQP Channel
    .spread((lastEventNumber, ok) => Promise.all([
      lastEventNumber, this.amqpConn.createChannel()
    ]))

    // Assert events Exchanage
    .spread((lastEventNumber, channel) => Promise.all([
      lastEventNumber, channel,
      channel.assertExchange(this.options.amqp.exchange, 'topic', {durable: true}),
    ]))

    // Publish to AMQP
    .spread((lastEventNumber, channel, ok) => {
      const published = events.map(event => channel.publish(
        this.options.amqp.exchange, streamId, new Buffer(JSON.stringify(event))
      ))

      // If any publish-call returned false, we're in trouble…
      const publishedOk = published.reduce((prev, curr) => (prev & curr), true)

      return {
        result:           0,
        firstEventNumber: events[0].eventNumber,
        lastEventNumber:  lastEventNumber,
        publishedOk:      publishedOk,
      }
    })
}

EspPrototype.ExpectedVersion = {
  Any: -2,
  NoStream: -1,
}

function TimeoutError(message) {
  this.name    = 'TimeoutError'
  this.message = message
}
TimeoutError.prototype = Error.prototype

module.exports = Esp
module.exports.TimeoutError = TimeoutError
module.exports.EspPrototype = EspPrototype // Exposing for tests.
