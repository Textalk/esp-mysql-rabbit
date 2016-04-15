'use strict'

const EventStorePromise = require('../esp')
const async             = require('asyncawait/async')
const await             = require('asyncawait/await')
const _                 = require('highland')

// A convenient wrapper to await the result of a highland stream.  Always gives an array though.
const await_  = stream => await(new Promise((res, rej) => stream.errors(rej).toArray(res)))

// A stub applyEvent that justs take the event object and boils down the latest value of each key.
const applyEvent = (state, event) => {
  console.log('Received and applying event:', event)
  return Object.assign({}, state, event)
}

// Stream to boil down.
const streamId   =  'testStream'

async(() => {
  // Connect to EventStore.
  const esp = await(EventStorePromise({
    mysql: {
      user:     'esp',
      password: 'esp',
      database: 'esp',
    },
    amqp: {
      url:      'amqp://localhost',
      exchange: 'events',
    }
  }, {timeout: 10000}))

  // Try adding some events asynchronously.
  setInterval(async(() => {
    const event = {foo: 'bar', mydate: Date()}
    console.log('Adding event in stream', streamId)
    await(esp.writeEvents(streamId, esp.ExpectedVersion.Any, false,
                          [{eventId: esp.createGuid(), eventType: 'test', data: event}]))
    console.log('Event is now added asynchronously and awaited.')
  }), 1000)

  try {
    //const stream = esp.readStreamEventsUntil(streamId, 0, 10)
    //stream.on('data', event => {
    //  console.log('Got data on non highland stream')
    //})


    // Read all events up until event 10.  This will wait for events to come in.
    const result = await_(_(esp.readStreamEventsUntil(streamId, 0, 10)).reduce(0, applyEvent))
    console.log('Got result', result)
  } catch (err) {
    // Catching errors, probably timeout for too few events.
    console.log('Caught error', err)
  }
  console.log('\nAll done.')
  process.exit()
})().done()
