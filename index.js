/* global require */
/* global module */
/* global process */

var weblogMongodb = function(setup) {
  setup.host  = setup.host ? setup.host : require('ip').address()
  setup.topic = setup.domain+'.'+setup.host+'.'+setup.service

  var _ = require('lodash')
  var when = require('when')
  var MongoClient = require('mongodb').MongoClient
  var autobahn = require('autobahn')

  var connection = new autobahn.Connection({
    url: process.argv[2] || 'ws://127.0.0.1:8080/ws',
    realm: process.argv[3] || 'weblog'
  })

  var db
  var main = function(session) {

    MongoClient.connect(setup.dbconnection, function(err, database) {
      if(err) throw err
      db = database

      var stream = db.collection(setup.headers[0].collection).find({}, {
          tailable : true,
          awaitdata : true,
          numberOfRetries : -1,
        }).stream()

      var publish2topic = false, val
      stream.on('data', function(data) {
        if (publish2topic && data) {
          val = _.values(_.pick(data, setup.headers[0].fields))
          session.publish(setup.topic, val)
        }
      })

      setTimeout(function() { publish2topic = true }, 10000)
    })

    session.subscribe('discover', function() {
      session.publish('announce', [_.pick(setup, 'domain', 'host', 'service', 'topic')])
    })

    session.register(setup.topic+'.header', function() {
      return setup.headers
    })

    session.register(setup.topic+'.reload', function(args) {
      var controls = args[0]
      if (controls.offset < 0) controls.offset = 0
      var table = controls.header
      var d = when.defer()
      var collection = db.collection(controls.header.collection)
      var val, res = []
      var selector = {}, begin = {}, end = {}, filter = {}

      if (controls.begin)  begin[table.fields[controls.rangefield]]   = { $gte: controls.begin }
      if (controls.end)    end[table.fields[controls.rangefield]]     = { $lte: controls.end }
      if (controls.filter) filter[table.fields[controls.filterfield]] = { $regex: new RegExp(controls.filter, 'i') }
      _.merge(selector, begin, end, filter)

  //console.log('selector', selector)
      collection.find(selector).skip(controls.offset).limit(controls.count).each(function(err, doc) {
        if(doc) {
          val = _.values(_.pick(doc, table.fields))
          res.push(val)
        } else {
          d.resolve(res)
        }
      })
      return d.promise
    })
  }

  connection.onopen = main

  connection.open()
}

module.exports = weblogMongodb
