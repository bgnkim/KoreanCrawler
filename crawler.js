var Crawler = require('crawler').Crawler;
var fs = require('fs');
var async = require('async');
var entity = require('entities');
var mkdirp = require('mkdirp');
var dateformat = require('dateformat');

var getHost = function(url){
  var protocol = url.indexOf('//', '');
  
  var cut = url.indexOf('/', protocol + 2);
  return url.slice(0, (cut < 0) ? url.length : cut);
};

var consistentTexts = function( text, write, finish ) {
  var index = text.search(/[\(\{\[<‘“"'\.\?\!”’>\]\}\)]{1}/);
  var prev = '', stack = [];

	var hasChar = function(){ return index > -1; };
	var whileReduce = function(callback) {
		var ch = text.charAt(index);

		prev += text.slice(0, index + 1);
		text = text.slice(index + 1);

		if ( ch === '.' || ch === '?' || ch === '!' ) {
			if ( stack.length === 0 ) {
				var prevCh = prev.charAt(prev.length - 2);
				if ( prevCh.search(/[0-9a-z]+/i) === -1 ){
					write(prev);
					prev = '';
				}
			}
		} else if ( stack.length > 0 ) {
			var peek = stack[stack.length - 1] + ch;

			switch ( peek ) {
				case '[]':
				case '()':
				case '{}':
        case '<>':
				case '‘’':
				case '“”':
        case '\'\'':
        case '""':
					stack.pop();
					break;
				default:
					if (ch.search(/[>\]\)\}’”]{1}/) > -1 ){
						break;
					}
					stack.push(ch);
			}
		} else {
			if (ch.search(/[>\]\)\}’”]{1}/) === -1 ){
				stack.push(ch);
			}
		}

		index = text.search(/[\(\{\[<‘“"'\.\?\!”’>\]\}\)]{1}/);
		callback();
	};
	
  async.whilst( hasChar, 
		whileReduce, function(){
		  write(prev + text);
			finish();
			
		  delete stack;
			delete index;
			delete prev;
		});
};

module.exports = function( visit, saveCallback ) {
  var self = this;
  
  var visitLink = function($, prev){
    async.reduce($('a[href], frame[src], iframe[src]'), [], function(urls, a, cb){
      var href = $(a).attr('href');
      if( !href ){
        href = $(a).attr('src');
      }
      
      if( href ){
        href = href.replace(/[a-z0-9_]+=&/gi, '').replace(/[a-z0-9_]+=$/gi, '').replace(/#.*$/, '').trim();
        var ext = href.slice(-5);
        
        if ( href.length !== 0 && href.search(/^javascript:/i) === -1 
             && href.search(/search/i) === -1 && href.search(/(log|sign)in[\.a-z]*/i) === -1
             && ext.search(/\.(flv|wmv|avi|hwp|pdf|xls|xlsx|ppt|pptx|doc|docx|mov|mpg|mp3|mp4|zip|rar|7z|gz|bz|alz|exe|dmg|apk|deb)$/) === -1 ){
          if ( href.search(/^http/i) > -1 ){
            urls.push(href);
          }else if ( href.charAt(0) === '/' ){
            urls.push(prev.host + href);
          }else if ( href.search(/^[a-z\.]+[\?\/]?/i) > -1 ){
            urls.push(prev.cwd + '/' + href);
          }else{
            if ( href.search(/^http/i) === -1 ){
              href = 'http://'+ href;
            }
            urls.push(href);
          }
        }
      }
      
      if(urls.length > 100){
        visit(urls);
        cb(false, []);
      }else{
        cb(false, urls);
      }
    }, function(err, urls){
      visit(urls);
    });
  };
  
  var callback = function( $, uri, headers, body ) {
    var date = dateformat('yyyymmddHH');
    uri = unescape(uri);
    
    var prev = {
      host: getHost(uri),
      cwd: uri.slice(0, uri.lastIndexOf('/')),
      visited: false
    };
    //var urls = './crawled/url';//'./texts/' + host + '/urls';
    var text = 'B:/korean/crawled/sentence'+date;//'./texts/' + host + '/text';
    //var orig = './texts/' + host + '/original';
    
    //mkdirp.sync('./texts/'+host);
    
    //fs.appendFile(urls, uri + '\n');
    //fs.appendFile(orig, JSON.stringify({uri: uri, header:_.omit(headers, 'set-cookie'), body:body.replace(/[\n\t\r]+/g,'')}) + '\n' );
    
    $('script,link,embed').remove();
      
    var br = $('br');
    if (br !== null){
      br.append('\n');
    }
    
    var deco = $('span, em, strong, ins, del, i, u, b');
    if (deco !== null){
      deco.prepend(' ');
      deco.append(' ');
    }
    
    var collectLinks = function(){
      if ( !prev.visited ){
        prev.visited = true;
        visitLink($, prev);
      }
    };
    
    async.reduce($('p, q, blockquote, h1, h2, h3, h4, h5, h6'), '', function(str, p, cb){
      var t = entity.decodeHTML($(p).text()).trim();
      
      if ( t.search(/[가-힣]{3,}/) > -1 ){
        collectLinks();
        consistentTexts(t.replace(/\s+/g, ' ').trim(), function(text){
          if ( text.length > 5 ){
            str = str + text.trim() + '\n';
          }
        }, function(){
          cb(false, str);
        });
      }else{
        cb(false, str);
      }
    }, function(err, str){
      fs.appendFile(text, str, function(err){
        if(err){
          console.log(err);
        }
      });
    });
  };
  
  self.crawler_ = new Crawler({
    skipDuplicate : true,
    callback : function(e, r, $){
      if($){
        var jQ = $, uri = r.uri, headers = r.headers, body = r.body;
        callback(jQ, uri, headers, body);
      }
    },
    followRedirect : true,
    maxRedirects: 2,
    forceUTF8: true,
    timeout : 10000,
    retries : 2,
    retryTimeout : 5000,
    onDrain : saveCallback
  });
  
  self.insert = function( url ) {
    process.nextTick(function(){
      self.crawler_.queue(url);
    });
  };
  
  process.send({
    assign: true
  });
};
