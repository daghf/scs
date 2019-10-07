/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 Norsk Rikskringkasting AS. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

const http  = require('http');
const https = require('https');



global.debug = function debug(...args) {
  if (global.verbose) {
    log(...args);
  }
}



global.log = function log(...args) {
  console.log(new Date().toISOString() + ": ", ...args);
}    



global.jsonpretty = function jsonpretty(json) {
  return JSON.stringify(json, null, 2);
}



global.jsondebug = function jsondebug(json) {
  if (! global.verbose) {
    return "";
  }
  return jsonpretty(json);
}



global.parseIntArg = function parseIntArg(args, arg) {
  if (! args.hasOwnProperty(arg)) {
    log('Error: args["' + arg + '"] is undefined');
    proess.exit(1);
  }
  let v = parseInt(args[arg], 10);
  if (isNaN(v)) {
    console.log("Unable to parse integer parameter " + arg);
    process.exit(1);
  }
  return v;
}



global.parseFloatArg = function parseFloatArg(args, arg) {
  if (! args.hasOwnProperty(arg)) {
    log('Error: args["' + arg + '"] is undefined');
    proess.exit(1);
  }
  let v = parseFloat(args[arg], 10);
  if (isNan(v)) {
    console.log("Unable to parse float parameter " + arg);
    process.exit(1);
  }
  return v;
}



global.parsePctArg = function parsePctArg(args, arg) {
  if (! args.hasOwnProperty(arg)) {
    log('Error: args["' + arg + '"] is undefined');
    proess.exit(1);
  }
  let v = parseInt(args[arg], 10);
  if (isNaN(v) || v > 100 || v < 0) {
    console.log(arg + " parameter must be between 0 and 100");
    process.exit(1);
  }
  return v;
}



global.getRandomInt = function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}



global.initHttpsAgent = function initHttpsAgent() {
  return new https.Agent({ keepAlive: true });
}



global.initHttpAgent = function initHttpAgent() {
  return new http.Agent({ keepAlive: true });
}



global.isHttps = function isHttps(url) {
  return ("https" === url.substring(0, 5) ? 1 : 0);
}



global.isRedirect = function isRedirect(statusCode) {
  return (statusCode === 301 ||
          statusCode === 302 ||
          statusCode === 307 ||
          statusCode === 308);
}



global.parseUrl = function parseUrl(url) {
  let result = {};
  let parts = url.split("://");
  let firstslash;
  let portsep;
  
  firstslash = parts[1].indexOf("/");
  result.proto    = parts[0];
  result.hostname = parts[1].substring(0, firstslash);

  portsep = result.hostname.indexOf(":");
  if (portsep != -1) {
    result.port = result.hostname.substring(portsep + 1);
    result.hostname = result.hostname.substring(0, portsep);
  }

  result.path     = parts[1].substring(firstslash);
  
  return result;
}



global.doSendRequest = function doSendRequest(url, options,
                                              callback, discardBody) {
  let urlOpts = parseUrl(url);
  Object.keys(urlOpts).forEach(function(key) {
    options[key] = urlOpts[key];
  });
  options.followRedirect = false;
  
  let requestMethod;
  let thisRequest;
  
  if (isHttps(url)) {
    options.agent = options.https_agent;
    requestMethod = https.request;
  } else {
    options.agent = options.http_agent;
    requestMethod = http.request;
  }
  
  if (options.bindIp !== "0.0.0.0") {
    options.localAddress = options.bindIp;
  }
  
  try {
    let req = requestMethod(options, function (res) {
      let body = '';
      res.on('data', function(d) {
        if (! discardBody) {
          body += d;
        }
      });
      res.on('end', function() {
        callback(null, res, body);
      });
      res.on('error', function (e) {
        callback(e, null, null);
      });
    });
    thisRequest = req;
    req.on('error', function (e) {
      callback(e, null, null);
    });
    req.end();
  } catch(e) {
    console.log("sendRequest() error: " + e);
  }
  
  return thisRequest;
}
