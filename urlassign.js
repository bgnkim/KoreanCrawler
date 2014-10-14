var _ = require('underscore');
var async = require('async');
var dateformat = require('dateformat');
var numeral = require('numeral');
var mongoose = require('mongoose');
var fs = require('fs');

mongoose.connect('mongodb://localhost/crawler');
var Queue = mongoose.model('Queue', new mongoose.Schema({
  u: {type: String, unique: true, sparse: true}, 
  v: Boolean
}));
  
module.exports = function(){
  var self = this;
  
  self.time = {
    setup: new Date(),
    now: new Date()
  };
  self.count = {
    assign: 0,
    passed: 0
  };
  
  self.save = function(){
    fs.writeFile('./status_url', JSON.stringify(self.count), function(err){
      if(err){
        console.log('STACK SAVE FAILED', err);
      }else{
        console.log('STACK SAVE FINISHED');
      }
    });
  };
  
  self.load = function(){
    fs.readFile('./status_url', function(err, data){
      if(data){
        data = JSON.parse(data);
        self.count = data;
      }else{
        self.visit("http://navercast.naver.com");
      }
      console.log("LOAD COMPLETED");
    });
  };
  
  self.visit = function(url){
    url = url.charAt(url.length - 1) === '/' ? url.slice(0, url.length - 1) : url;
    
    var item = new Queue({
      u : url,
      v : false
    });
    item.save(function(err){
      if(!err){
        self.count.assign ++;
      }
    });
  };
  
  self.assign = function(control){
    console.log("WORK START");
    self.control = control;
    self.request_();
  };
  
  self.request_ = function(){
    var now = new Date();
    if (now - self.time.now > 1000 * 60) {
      self.time.now = now;
      self.count.remain = self.count.assign - self.count.passed;
      console.log(dateformat(self.time.now, 'HH:MM:ss'), numeral(self.count.assign).format('0,0'),'-', numeral(self.count.passed).format('0,0'), 'REMAIN', numeral(self.count.remain).format('0,0'), 'ETA', numeral((self.time.now - self.time.setup) / self.count.passed * self.count.remain / 1000).format('00:00:00'), 'WORKER', self.control.count());
      
      if (Math.floor((now - self.time.setup) / 1000 * 60) % 10 === 0){
        self.save();
        self.control.save();
      }
    }
    
    var req = self.control.request();
    if (req){
      Queue.findOneAndUpdate({v: false}, {v: true}, function(err, item){
        if ( !err && item !== null ){
          try{
            self.count.passed ++;
            req.uri = item.u;
            req.send({
              assigned: true,
              uri: item.u
            });
          }catch(e){
            self.count.passed --;
            item.v = false;
            item.save();
          }
        }else{
          self.control.finish(req);
        }
        process.nextTick(self.request_);
      });
    }else{
      setTimeout(self.request_, 100);
    }
  };
  
  self.load();
};