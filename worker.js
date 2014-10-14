var _ = require('underscore');
var async = require('async');
var fs = require('fs');

module.exports = function(cluster, handler){
  var self = this;
  
  self.assigned = [];
  self.idle = [];
  self.handler = handler;
  self.exception = [];
  
  self.save = function(){
    async.reduce(self.assigned, [self.exception], function(urls, worker, next){
      if(worker.uri !== null){
        urls.push(worker.uri);
      }
      next(false, urls);
    }, function(err, urls){
      fs.writeFile('./status_worker', JSON.stringify(urls), function(err){
        if(err){
          console.log('WORKER SAVE FAILED', err);
        }else{
          console.log('WORKER SAVE FINISHED');
        }
      });
    });
  };
  
  self.load = function(){    
    fs.readFile('./status_worker', function(err, data){
      if (data){
        data = JSON.parse(data);
        self.exception = _.flatten(self.exception, data);
      }
    });
  };
  
  self.assign = function(){
    var worker = cluster.fork();
    worker.cnt = 0;
    worker.on('message', function(msg){
      self.handler(worker, msg);
    });
    worker.on('exit', function(code, signal){
      if(worker.uri){
        fs.appendFile('./crawled/exceptions', signal+'\t'+worker.uri+'\n');
      }
      self.assign();
      self.assigned = _.without(self.assigned, worker);
    });
    self.assigned.push(worker);
  };
  
  self.request = function(){
    if ( self.assigned.length < 12 ) {
      self.assign();
    }
    
    return self.idle.shift();
  };
  
  self.kill = function(worker){
    setTimeout(function(){
      try{
        worker.send({
          assigned: false,
          disconn: true
        });
      }catch(e){
        console.error(e);
      }
    }, 10);
  };
  
  self.finish = function(worker, isAssign){
    worker.uri = false;
    worker.cnt ++;
    
    if ( isAssign ){
      var exception = self.exception.shift();
      if(exception){
        worker.uri = exception;
        worker.send({
          assigned: true,
          uri: worker.uri
        });
      }else{
        self.idle.push(worker);
      }
    }else{
      self.kill(worker);
    }
  };
  
  self.count = function(){
    return (self.assigned.length - self.idle.length) +' of '+ self.assigned.length;
  };
  
  self.load();
};