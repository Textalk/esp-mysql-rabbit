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
    .then(result => result[0].consume(
      result[1],
      msg => stream.push(JSON.parse(msg.content))
    ))
    .then(ok     => stream.emit('open'))
    .catch(err => stream.emit('error', err))

  return stream
}

module.exports = Esp
