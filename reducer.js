var _ = require('underscore');
var async = require('async');
var dateformat = require('dateformat');
var fs = require('fs');
var crypto = require('crypto');

var read = [];

var Reader = function(){
  var self = this;
  
  self.read = function(file, next){
    read.push(file);
    
    var stream = fs.createReadStream(file, {
      encoding:'UTF-8'
    });
    
    var string = '';
    stream.count = [];
    
    var close = function(){
      if (stream.count.length > 0){
        setTimeout(close, 1);
      }else{
        console.log('FINISH READING', file);
        next();
      }
    };
    
    console.log('START READING', file);
    
    stream.on('data', function(chunk){
      string += chunk;
      var splited = string.split('\n'); 
      string = splited.pop();
      stream.count.push(false);
      async.eachSeries(splited, function(sentence, step){
        self.write(sentence, step);
      }, function(){
        stream.count.pop();
      });
    });
    
    stream.on('end', function(){
      var splited = string.split('\n'); 
      string = splited.pop();
      async.eachSeries(splited, function(sentence, step){
        self.write(sentence, step);
      }, close);
    });
  };
  
  self.write = function(string, step){
    string = self.getConsistent(string);
    if (string.length > 0){
      var filename = 'D:/korean/crawled/hash/'+self.hash(string);
      fs.appendFile(filename, string+'\n', {encoding:'UTF-8'}, function(err){
        if(err){
          console.error(err);
        }
        step();
      });
    }else{
      step();
    }
  };
  
  self.reduce = function(callback){
    fs.readdir('D:/korean/crawled/hash', function(err, list){
      async.eachSeries(list, function(path, next){
        fs.readFile('D:/korean/crawled/hash/'+path, function(err, data){
          if(!err){
            var array = data.split('\n');
            array = _.uniq(array).sort();
            var str = array.join('\n') + '\n';
            fs.writeFile('D:/korean/crawled/hash/'+path, str, function(err){
              if(err){
                console.log(err)
              }
              next();
            });
          }
        });
      }, callback);
    });
  };
  
  self.getConsistent = function(str){
    str = str.replace(/[a-z0-9]+\*+/gi, '').replace(/[\u201E-\u202F\u2032-\u209F\u20BE-\u20FF\u2139-\uABFF]+/,'');
    if (str.search(/[가-힣]+\s[가-힣]+/) > -1){
      return str;
    }else{
      return '';
    }
  };
  
  self.hash = function(str){
    var checksum = crypto.createHash('md5');
    checksum.update(str);
    return checksum.digest('hex').slice(-3);
  };
};

var ticker = function(){
  fs.readdir('B:/korean/crawled', function(err, list){
    var newFiles = [];
    if ( !err ){
      var newFiles = _.difference(list, read);
      newFiles = _.sortBy(newFiles, function(item){
        return item.slice(8);
      });
      newFiles.pop();
      newFiles = newFiles.slice(0, 3);
      newFiles = _.map(newFiles, function(file){
        return 'B:/korean/crawled/'+file;
      });
    }
    
    var reader = new Reader();
    async.eachSeries(newFiles, reader.read, function(){
      console.log(dateformat('HH:MM:ss'), 'READING FINISHED, REDUCING START');
      reader.reduce();
      console.log(dateformat('HH:MM:ss'), 'WAIT 1 Minutes');
      setTimeout(ticker, 1000 * 60);
    });
  });
};

ticker();