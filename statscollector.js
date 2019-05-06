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

require('./scs-helpers.js');

const https = require('https');



class StatsCollector {
  // A singleton object of this class counts how many streams have
  // seen various phenomena during a reporting period.
  //
  // Currently counts:
  //
  // - Number of streams that have seen segments arrive later than a
  //   whole buffer refresh wait period, but did not have to pause
  //   due to buffering (nSlowServerSeen)
  //
  // - Number of streams that have had to pause while waiting for
  //   data (nBadService)
  //
  // - Number of streams that have been paused for more than a whole
  //   segment play length while waiting for data (nVeryBadService).
  //
  // The StreamManager periodically tells StatsCollector to report
  // its data to Influx. The counters are then reset.
  
  constructor(streamManager, config) {
    this.sm = streamManager;
    this.config = config;
    this.stats = {};
    this.influxTag = streamManager.influxTag;
    this.streamHost = streamManager.streamHost;
    debug("StatsCollector constructed");
  }
  
  
  
  registerBadService(clientId) {
    this.registerObservation(clientId,"bad");
  }
  
  
  
  registerVeryBadService(clientId) {
    this.registerObservation(clientId, "veryBad");
  }
  
  
  
  registerSlowServer(clientId) {
    this.registerObservation(clientId, "slow");
  }
  
  
  
  registerObservation(clientId, key) {
    if (! this.stats[clientId]) {
      this.stats[clientId] = {};
    }
    this.stats[clientId][key] = 1;
  }
  
  
  
  calculateStats() {
    let result = {};
    debug("calculateStats running");
    result["nStreams"]        = this.sm.nStreams;
    result["nVeryBadService"] = this.countServiceStatus('veryBad');
    result["nBadService"]     = this.countServiceStatus('bad');
    result["nSlowServerSeen"] = this.countServiceStatus('slow');
    
    return result;
  }
  
  
  
  countServiceStatus(key) {
    let theSC = this;
    let count = 0;
    
    debug("countServiceStatus, stats: " + jsondebug(this.stats));
    
    Object.keys(this.stats).forEach(function(s) {
      debug("countServiceStatus, s: " + s);
      if (theSC.stats[s] && theSC.stats[s][key]) {
        count++;
      }
    });
    
    debug("countServiceStatus(" + key + "): " + count);
    
    return count;
  }
  
  
  
  sendStatistics() {
    this.toInflux(this.calculateStats());
    this.resetStats();
  }
  
  
  
  resetStats() {
    this.stats = {};
  }
  
  
  
  toInflux(stats) {
    let theSC = this;
    let config = this.config;
    let statsKeys = Object.keys(stats);
    
    debug("toInflux running");
    
    if (!(config.hasOwnProperty('influx_user'))) {
      log("influx_user not configured, skipping influx job");
      return;
    }
    
    let nodeOpt = {
      hostname: config.influx_host,
      path: "/write?db=" +
        encodeURIComponent(config.organization),
      method: "POST",
      auth: config.influx_user + ":" + config.influx_pw
    };
    let influxData = [];
    
    statsKeys.forEach(function(key) {
      debug("statsKeys key: " + key);
      influxData.push("scs,hcdn_node_id=" + theSC.influxTag +
                      ",hostname=" + theSC.streamHost +
                      ",scs_pid=" + process.pid + " " +
                      key + '=' + stats[key]);
    });
    
    let combined = influxData.join("\n");
    debug("sending to influx: \n" + combined);
    
    try {
      let req = https.request(nodeOpt, function (res) {
        let body = '';
        res.on('data', function(d) {
          body += d;
        });
        res.on('end', function() {
          if (res.statusCode > 299) {
            console.log("toInflux()::res failure: " +
                        res.statusCode);
            console.log(body);
          }
        });
        res.on('error', function (e) {
          console.log("toInflux()::res error: " + e);
        });
      });
      req.on('socket',function(socket){
        socket.setTimeout(2000,function(){
          req.abort();
        });
      });
      req.on('error', function (e) {
        console.log("toInflux()::req error: " + e);
      });
      req.write(combined);
      req.end();
    } catch(e) {
      console.log("toInflux() error: " + e);
    }
  }
}



module.exports = StatsCollector;
