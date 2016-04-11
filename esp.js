'use strict'

const Promise = require('bluebird')

const mysql = require('mysql')
Promise.promisifyAll(require("mysql/lib/Pool").prototype)
Promise.promisifyAll(require("mysql/lib/Connection").prototype)
const EspPrototype = {}

const defaultDefaults = {
  timeout:        2000,
  resolveLinkTos: false,
  requireMaster:  false,
  maxCount:       4095,
}

const Esp = (options, defaults) => {
  const esp = Object.create(EspPrototype)

  esp.options  = options
  esp.defaults = Object.assign({}, defaultDefaults, defaults)

  return Promise.resolve()
    // Connect to MySQL and Amqp
    .then(() => Promise.all([
      Promise.promisifyAll(mysql.createConnection(options.mysql)).connectAsync(),
      amqp.connect(options.amqp).then(amqpConn => amqpConn.createChannel()),
    ]))
    .spread((mysqlConn, amqpChannel) => {
      esp.mysqlConn = mysqlConn
      esp.amqpChannel = amqpChannel

      // Make sure there is an event exchange in amqp.
      esp.amqpChannel.assertExchange(options.amqp.exchange, 'fanout', {durable: true})

      return esp
    })
}

module.exports = Esp
