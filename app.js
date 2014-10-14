var cluster = require('cluster');

if ( cluster.isMaster ) {
  var UrlAssign = require('./urlassign')
  var stack = new UrlAssign();
  
  var WorkerAssign = require('./worker');
  var async = require('async');
  var control = new WorkerAssign(cluster, function( worker, msg ){
    if ( msg.link ) {
      async.each(msg.link, function(url, next){
        stack.visit(url);
        process.nextTick(next);
      });
    } else if ( msg.done || msg.assign ){
      control.finish(worker, msg.assign);
    }
  });
	
  stack.assign(control);
} else {
  var lastMessage = {};
  var disconn = false;
  var _ = require('underscore');
  
  var Crawler = require('./crawler');
  var crawler = new Crawler(function( urls ) {
    lastMessage = {
      done : false,
      link : urls
    };
    process.send(lastMessage);
  }, function() {
    if ( process.memoryUsage().heapTotal >= 500000000 ){
      lastMessage = {
        done : true
      };
    } else {
      lastMessage = {
        assign : true
      };
    }
    process.send(lastMessage);
  });

  var msgHandler = function ( msg ) {
    if ( msg.assigned ){
      crawler.insert(msg.uri);
    } else if ( msg.disconn ){
      process.disconnect();
    }
  };
  
  process.on('message', msgHandler);
}