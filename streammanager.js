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

require("./scs-helpers.js");

const EventEmitter   = require('events');
const Cooloff        = require("./cooloff.js");
const StatsCollector = require("./statscollector.js");
const AttachedStream = require("./attachedstream.js");

const fs           = require('fs');
const Promise      = require('promise');

let read = Promise.denodeify(fs.readFile);

//
// Class to manage multiple streams. Will start up to maxStreams
// streams. When a stream terminates, it will start up another, until
// all lines in the URL file have been read.
//
class StreamManager extends EventEmitter {
  constructor(args) {
    super();
    
    debug("StreamManager constructor running");
    
    let streamHost;
    
    this.config = this.getConfig();
    
    // Arguments
    for (let arg of Object.keys(args)) {
      this[arg] = args[arg];
    }
    
    this.streamUrl         = args["streamUrl"]
    this.immediateStreams  = args["immediateFactor"]        ?
      Math.round(this.maxStreams * args["immediateFactor"]) :
      0;
    
    // magic values
    this.influxJobInterval     = 60;
    this.utilizationInterval   = 5;
    this.utilizationOK         = true;
    this.newClientInterval     = 25;
    this.newClientRampInterval = 10;
    this.startupDelay *= 1000; // to ms
    
    this.urls = [];
    this.streams           = {};
    this.attachedStreams   = {};
    this.unattachedStreams = {};
    this.nSkippedLines      = 0;
    this.nUnattachedStreams = 0;
    this.nStreams           = 0;
    this.oldestLiving       = 0;
    this.streamSerialNo     = 0;
    this.haveSeenSlowness   = 0;
    this.haveSeenOverload   = 0;
    
    this.maxStreamsCooloff  = new Cooloff("maxStreams",  3);
    this.killStreamsCooloff = new Cooloff("killStreams", 2);
    this.slownessCooloff    = new Cooloff("slowness",    2);
    this.stats              = new StatsCollector(this, this.config);
    
    this.https_agent = initHttpsAgent();
    this.http_agent  = initHttpAgent();
    
    this.on('streamClientAttached',       this.clientAttached);
    this.on('streamClientAborted',        this.clientAborted);
    this.on('streamClientTerminated',     this.clientTerminated);
    this.on('streamClientKilled',         this.clientKilled);
    this.on('streamClientRedirected',     this.clientRedirected);
    this.on('streamClientVeryBadService', this.clientVeryBadService);
    this.on('streamClientBadService',     this.clientBadService);
    this.on('streamClientSlowServer',     this.slowServer);
    
    debug("INFLUX TAG: " + this.influxTag);
    if (this.influxTag) {
      this.setupInfluxJob();
    }
    if (this.useUtilizationFile) {
      this.setupUtilizationJob();
    }
  }
  
  
  
  getConfig() {
    let config = {};
    
    try {
      let cfgfile = fs.readFileSync("scs.conf");
      config = JSON.parse(cfgfile);
    } catch(e) {
      console.log("Unable to read config file scs.conf: " );
      console.log(e);
    }
    return config;
  }
  
  
  
  writeResumeState(oldestLN) {
    let file = this.resumeState;
    if (!file) {
      return
    }
    let cb = function(err) {
      if (err) {
        log("Unable to write resume state file \"" + file + "\": " + err);
      } else {
        log("Wrote resume state " + oldestLN + " to " + file);
      }
    };
    
    fs.writeFile(file, String(oldestLN) + "\n", cb);
  }
  
  
  
  setupInfluxJob() {
    let theSM = this;
    let sc    = this.stats;
    
    let influxJob = function () {
      sc.sendStatistics();
      debug("influxJob: interval: " + theSM.influxJobInterval * 1000);
      setTimeout(influxJob, theSM.influxJobInterval * 1000);
    };
    
    let delay = Math.random() * this.influxJobInterval * 1000;
    debug("Running influx job in " + delay/1000 + " seconds");
    setTimeout(influxJob, delay);
  }
  
  
  
  setupUtilizationJob() {
    let theSM = this;
    let utilizationJobCB = function () {
      theSM.utilizationJob();
      setTimeout(utilizationJobCB, theSM.utilizationInterval * 1000);
    };
    
    let delay = Math.random() * this.utilizationInterval * 1000;
    debug("Running utilization job in " + delay/1000 + " seconds");
    setTimeout(utilizationJobCB, delay);
  }
  
  
  
  utilizationJob() {
    let sm = this;
    let url = "https://" + this.streamHost + "/utilization.json";
    
    let responseCallback = function(err, res, body) {
      if (err) {
        log(': Utilization request failed: ' + err.message);
        return;
      }
      
      if (res.statusCode === 200) {
        try {
          let u = JSON.parse(body);
          debug("Utilization data: " + body);
          sm.utilizationOK = u.sys_utilization < sm.utilizationMax;
          debug("utilizationOK: " + sm.utilizationOK);
        } catch (err) {
          log("Unable to parse utilization file: " + err);
          return;
        }
      } else {
        log("Utilization request not ok: " + res.statusCode + ", url: " + url);
        return;
      }
    };
    
    this.sendRequest(url, responseCallback);
  }
  
  
  
  dynamicMaxLoadEnabled() {
    return this.dynamicMaxLoad;
  }
  
  
  
  debugStreamState(id) {
    if (! global.verbose) {
      return;
    }
    debug(id + ": unattachedStreams: " + JSON.stringify(this.unattachedStreams));
    debug(id + ": attachedStreams:   " + JSON.stringify(this.attachedStreams));
  }
  
  
  
  clientAttached(client) {
    debug(client.id + ": Stream client attached");
    this.debugStreamState(client.id);
    
    this.nStreams++;
    this.nUnattachedStreams--;
    this.attachedStreams[client.id] = 1
    
    if (this.unattachedStreams[client.id]) {
      delete this.unattachedStreams[client.id];
    } else {
      log(client.id + ": Attempt to delete nonexistent unattached stream");
      process.exit(-1);
    }
    
    
    this.streams[client.id] = client;
    this.debugStreamState(client.id);
    debug("nStreams: " + this.nStreams +
          ", nUnattachedStreams: " + this.nUnattachedStreams +
          ", maxStreams: " + this.maxStreams);
  }
  
  
  
  clientTerminated(client) {
    log(client.id + ": Stream client terminated");
    this.clientTerminatedOrKilled(client);
    this.startAStream();
  }
  
  
  
  clientKilled(client) {
    log(client.id + ": Stream client killed");
    this.clientTerminatedOrKilled(client);
  }
  
  
  
  clientRedirected(client) {
    log(client.id + ": Stream client redirected away");
    // Server is shedding load and sent this guy elsewhere.
    // Don't start a new stream.
    if (client.isAttached) {
      this.clientTerminatedOrKilled(client);
    } else {
      this.abortClient(client.id);
    }
  }
  
  
  
  unattachedClientRedirected(id) {
    log(id + ": Stream client redirected away");
    // Server is shedding load and sent this guy elsewhere.
    // Don't start a new stream.
    this.abortClient(id);
  }
  
  
  
  // This one takes the id as param instead of the whole stream
  // object, because the stream object may not have been created yet
  // when the abort occurred.
  clientAborted(id) {
    log(id + ": Stream client aborted");
    this.abortClient(id);
    this.startAStream();
  }
  
  
  
  abortClient(id) {
    this.debugStreamState(id);
    
    this.nUnattachedStreams--;
    
    if (this.unattachedStreams[id]) {
      delete this.unattachedStreams[id];
    } else {
      log(id + ": Attempt to delete nonexistent unattached stream");
      // process.exit(-1);
    }
  }
  
  
  
  clientTerminatedOrKilled(client) {
    if (client.isTerminated) {
      return;
    }
    
    this.debugStreamState(client.id);
    
    this.nStreams--;
    
    if (this.attachedStreams[client.id]) {
      delete this.attachedStreams[client.id];
    } else {
      log(client.id + ": Attempt to delete nonexistent attached stream");
      process.exit(-1);
    }
    
    delete this.streams[client.id];
    client.isTerminated = 1;
    
    this.maybeUpdateResumeState(client);
    
    this.debugStreamState(client.id);
    log(client.id +
        ": nStreams: "        + this.nStreams +
        ", nUnattachedStreams: " + this.nUnattachedStreams +
        ", maxStreams: "      + this.maxStreams +
        ", nVeryBadService: " + this.stats.countServiceStatus('veryBad') +
        ", nBadService: "     + this.stats.countServiceStatus('bad') +
        ", nSlowServer: "     + this.stats.countServiceStatus('slow'));
  }
  
  
  
  clientBadService(client) {
    if (client.isTerminating) {
      return;
    }
    
    this.stats.registerBadService(client.id);
  }
  
  
  
  clientVeryBadService(client) {
    if (client.isTerminating) {
      return;
    }
    
    this.stats.registerVeryBadService(client.id);
    this.killSomeStreams();
  }
  
  
  
  slowServer(client) {
    this.stats.registerSlowServer(client.id);
    this.reactToSlowness();
  }
  
  
  
  killSomeStreams() {
    if (! this.dynamicMaxLoad) {
      debug("Not --dynamic, skipping killSomeStreams");
      return;
    }
    
    let nToKill = Math.floor(this.nStreams * killFactor);
    
    debug("killSomeStreams, nToKill: " + nToKill);
    
    if (this.maxStreams === 0) {
      log("No streams left to kill, but decrementing maxStreams");
      this.reactToOverload();
      return;
    }
    
    if (! this.killStreamsCooloff.fire()) {
      log("Recently killed streams. Skipping now");
      // Killed streams recently. Let somebody else do it this time.
      return;
    }
    
    let currentStreamIds = [];
    let streamsToKill    = [];
    for (let id in this.streams) {
      currentStreamIds.push(id);
    }
    
    while (streamsToKill.length < nToKill) {
      debug("currentStreamIds: " + jsondebug(currentStreamIds));
      let pick = getRandomInt(0, currentStreamIds.length - 1);
      streamsToKill.push(currentStreamIds[pick]);
      debug("Pick: " + pick);
      debug("Selected " + currentStreamIds[pick]  + " for killing");
      currentStreamIds.splice(pick, 1); // remove the one we picked
      debug("streamsToKill: " + jsondebug(streamsToKill));
    }
    
    for(let i = 0; i < streamsToKill.length; i++) {
      let id = streamsToKill[i];
      log("Killing stream id " + id);
      // this.streams[id].terminate();
      this.streams[id].kill();
    }
    
    this.reactToOverload();
  }
  
  
  
  reactToSlowness() {
    this.haveSeenSlowness = 1;
  }
  
  
  
  reactToOverload() {
    this.reactToSlowness();
    this.haveSeenOverload = 1;
    this.maxStreams--;
  }
  
  
  
  maybeUpdateResumeState(client) {
    let sortf           = function (a, b) { return a - b };
    let oldestLivingKey = Object.keys(this.attachedStreams).sort(sortf)[0];
    let oldestLiving    = parseInt(oldestLivingKey, 10);
    
    if (! oldestLivingKey) {
      debug("oldestLivingKey is undef, no attached streams yet?");
      return;
    }
    
    if (! this.streams[oldestLivingKey]) {
      log("Error: Can not find oldest living stream, " +
          "oldestLivingKey: \"" + oldestLivingKey + "\"");
      debug("keys(this.streams): " + jsonpretty(Object.keys(this.streams)));
      this.debugStreamState(client.id);
    } else {
      let oldestLN     = this.streams[oldestLivingKey].lineNumber
      debug("Stream " + client.id + " died");
      debug("Previous oldest living: " + this.oldestLiving);
      debug("Current oldest living: " + oldestLiving);
      debug("Line number of oldest living stream: " + oldestLN);
      debug("oldestLivingKey: " + oldestLivingKey);
      
      if (this.oldestLiving < oldestLiving) {
        log("New oldest living: " + oldestLiving);
        this.oldestLiving = oldestLiving;
        this.writeResumeState(oldestLN);
      } else {
        log("Oldest living is unchanged");
      }
    }
  }
  
  
  
  runUrlFile(filename) {
    let sm = this;
    let p = read(filename, 'utf8')
        .then(function (data) { sm.startOneClientPerUrl(data) })
        .catch(function (err) {
          log("Read of " + filename + " failed: " + err);
        });
  }
  
  
  
  startOneClientPerUrl(data) {
    this.urls = data.split("\n");
    
    if (this.skipUrls > 0) {
      this.urls = this.urls.slice(this.skipUrls);
      this.streamSerialNo = this.skipUrls;
    }
    
    if (this.dynamicMaxLoad) {
      this.startClientsDynamically();
    } else {
      this.startFixedNumberOfClients();
    }
  }
  
  
  
  startClientsDynamically() {
    let sm = this;
    let job = function() {
      sm.startAStream();
    }
    
    while (this.immediateStreams-- > 0) {
      // Don't fire them up all at once to avoid killing the
      // server right away
      let delay = Math.random() * this.newClientRampInterval * 1000;
      log("Starting a client in " + delay/1000 + " seconds");
      setTimeout(job, delay);
    }
    
    this.startNewClientDynamically();
  }
  
  
  
  startFixedNumberOfClients() {
    let count = 0;
    let sm    = this;
    let job = function() {
      sm.startAStream();
    };
    
    while (count++ < this.maxStreams) {
      let delay = Math.random() * this.startupDelay;
      log("Starting client in " + delay/1000 + " seconds");
      setTimeout(job, delay);
    }
  }
  
  
  
  startNewClientDynamically() {
    // Try to maintain max possible load. Start up a new client at
    // intervals. When the server gets overloaded, kill a
    // percentage of the clients, then go back to starting new
    // ones incrementally again.
    let sm = this;
    let reduceRate = (this.haveSeenOverload || this.haveSeenSlowness);
    let delay = 1000 *
        (reduceRate ? this.newClientInterval : this.newClientRampInterval)
    
    
    let job = function() {
      log("nStreams: " + sm.nStreams +
          ", nUnattachedStreams: " + sm.nUnattachedStreams +
          ", maxStreams: " + sm.maxStreams);
      sm.startAStream();
      sm.startNewClientDynamically();
    }
    
    debug("startNewClientDynamically(), haveSeenSlowness: " +
          this.haveSeenSlowness);
    debug("startNewClientDynamically(), haveSeenOverload: " +
          this.haveSeenOverload);
    
    this.killStreamsCooloff.tick();
    this.adjustMaxStreams();
    
    delay = Math.random() * delay * 2; // random, but same average
    
    // When running many instances, one client per instance may be
    // too much before caches warm up etc. Extra long delay for
    // first client
    
    if (sm.nStreams === 0 && this.softStart) {
      delay *= 10;
    }
    
    log("Attempting start of new client in " + (delay/1000).toFixed(3) +
        " secs");
    setTimeout(job, delay);
    // Important: Adjust ramp rate after setting the timeout. If
    // slow server is seen during the delay, the low rate will be
    // used next time.
    this.adjustRampRate();
  }
  
  
  
  adjustMaxStreams() {
    this.maxStreamsCooloff.tick();
    if (this.nStreams === this.maxStreams) {
      if (this.maxStreamsCooloff.fire()) {
        this.maxStreams++;
      }
    }
    debug("maxStreams: " + this.maxStreams +
          ", nStreams: " + this.nStreams);
  }
  
  
  
  adjustRampRate() {
    this.slownessCooloff.tick();
    if (this.slownessCooloff.fire()) {
      this.haveSeenSlowness = 0;
    }
  }
  
  
  
  sendRequest(url, callback, discardBody) {
    let options = {
      https_agent        : this.https_agent,
      http_agent         : this.http_agent,
      rejectUnauthorized : this.ignoreCertErrors,
      bindIp             : this.bindIp
    };
    
    if (this.hostHeader) {
      options.headers = {
        Host: this.hostHeader
      };
    }
    
    doSendRequest(url, options, callback, discardBody);
  }
  
  
  
  startAStream() {
    // log("startAStream, this: " + JSON.stringify(this));
    // log("startAStream, this.urls: " + JSON.stringify(this.urls));
    
    if ((this.nStreams + this.nUnattachedStreams) >=
        this.maxStreams) {
      log((this.streamSerialNo + 1) +
          ": No room for more streams, nStreams: " + this.nStreams +
          " nUnattachedStreams: " + this.nUnattachedStreams +
          " maxStreams: " + this.maxStreams);
      return;
    }
    
    if (! this.utilizationOK) {
      log("Server is overloaded, not starting a new stream now");
      return;
    }
    
    let url
    if (this.streamPath) {
      url = this.streamPath
    } else {
      url = this.urls.shift();
    }
    
    if (! url) {
      log("No more urls to process");
      return;
    }
    
    url = this.prepareUrl(url);
    
    if (url.match(/\.f4m$/)) {
      log("HDS not supported, skipping");
      this.nSkippedLines++;
      this.startAStream();
      return;
    }
    
    let streamData = {};
    
    this.streamSerialNo++;
    
    let logstr = "startAStream(" + this.streamSerialNo + "): ";
    
    streamData.id         = this.streamSerialNo;
    streamData.lineNumber = this.streamSerialNo + this.nSkippedLines - 1;
    streamData.url        = url;
    
    log(logstr + "Url: " + streamData.url);
    
    this.nUnattachedStreams++;
    this.unattachedStreams[this.streamSerialNo] = 1;
    this.debugStreamState(this.streamSerialNo);
    this.createStreamClient(streamData);
    
    log(logstr + "nStreams: "  + this.nStreams +
        ", nUnattachedStreams: " + this.nUnattachedStreams +
        ", maxStreams: "       + this.maxStreams +
        ", nVeryBadService: "  + this.stats.countServiceStatus('veryBad') +
        ", nBadService: "      + this.stats.countServiceStatus('bad') +
        ", nSlowServerSeen: "  + this.stats.countServiceStatus('slow'));
  }
  
  
  
  startMultipleClientsSingleUrl() {
    if (! this.streamUrl) {
      console.log("No stream URL given. Exiting.");
      process.exit(1);
    }
    
    let id = 0
    while (this.nStreams +
           this.nUnattachedStreams < this.maxStreams) {
      let url = this.streamUrl
      this.createStreamClient({ id, url });
      id++;
    } 
  }
  
  
  
  prepareUrl(url) {
    let newUrl = "";
    let firstslash = url.indexOf("/");
    if (firstslash === 0) {
      debug("Url starts with a slash, it's a path!");
      let proto = this.protocol;
      let streamHost = this.streamHost;
      newUrl = this.protocol + "://" + this.streamHost + url;
    } else {
      debug("Url does not start with a slash, it had better be complete");
      newUrl = url;
    }
    
    return newUrl;
  }
  
  
  
  createStreamClient(streamData) {
    debug(streamData.id + ": createStreamClient, url: " + streamData.url);
    
    this.fetchQualityData(streamData);
  }
  
  
  
  fetchQualityData(streamData) {
    let sm = this;
    let id = streamData.id;
    
    let responseCallback = function(err, res, body) {
      if (err) {
        log(id + ': Request failed (' + streamData.url + "): " + err.message);
        sm.clientAborted(id);
        return;
      }
      
      if (res.statusCode === 200) {
        let url = streamData.url;
        streamData.baseUrl = url.substring(0, url.lastIndexOf("/"));
        try {
          sm.parseQualityData(streamData, body);
        } catch (err) {
          log(id + ": Unable to parse quality data: " + err);
          sm.clientAborted(id);
          return;
        }
      } else if (isRedirect(res.statusCode)) {
        log(id + ": HTTP REDIRECT: " + res.statusCode + ", url: " +
            streamData.url);
        sm.unattachedClientRedirected(id);
        return;
      } else {
        log(id + ": HTTP NOT OK: " + res.statusCode + ", url: " +
            streamData.url);
        sm.clientAborted(id);
        return;
      }
    };
    
    debug(id + ": streamData.url: " + streamData.url);
    this.sendRequest(streamData.url, responseCallback);
  }
  
  
  
  parseQualityData(streamData, qualityData) {
    let id = streamData.id;
    let sm = this;
    let prev;
    let cur;
    let qualityList = [];
    
    // log("\nHTTP OK:\n\n" + qualityData)
    
    qualityData.split("\n").forEach(function(s) { 
      prev = cur;
      cur = s;
      // log("prev: " + prev)
      // log("cur: " + cur)
      if (cur[0] !== '#') {
        let bw_match = prev.match(/BANDWIDTH=(\d+)/);
        if (bw_match) {
          let quality = {'url' : streamData.baseUrl + "/" + cur,
                         'bw'  : parseInt(bw_match[1])};
          qualityList.push(quality);
        } else {
          debug(id + ": no match");
        }
      }
    });
    
    if (qualityList.length === 0) {
      log(id + ": Quality data not found!");
    } else {
      debug(id + ": Bandwidth list: " + jsondebug(qualityList));
    }
    
    this.selectQuality(streamData, qualityList);
  }
  
  
  
  selectQuality(streamData, qualityList) {
    let id = streamData.id;
    let selected;
    let best;
    
    best = this.getBestQuality(id, qualityList);
    
    if (this.skipAudioOnly && best.bw < 200000) {
      // This is audio only
      log(id + ": Radio stream, skipping");
      this.clientAborted(id);
      return;
    }
    
    if (this.bw === "random") {
      let randomIndex = getRandomInt(0, qualityList.length - 1);
      selected = qualityList[randomIndex];
      log(id + ": Quality: " + JSON.stringify(selected));
    } else if (this.bw === "biased") {
      let r1 = getRandomInt(1, 100);
      if (r1 < 70) {
        log(id + ": bw=biased, selected best quality");
        selected = best;
      } else {
        let randomIndex = getRandomInt(0, qualityList.length - 1);
        selected = qualityList[randomIndex];
        log(id + ": bw=biased, quality: " + JSON.stringify(selected));
      }                
    } else if (this.bw === "best") {
      selected = best;
    }
    
    debug(id + ": New stream");
    let attachedStream
        = new AttachedStream(streamData,
                             this,
                             selected["url"],
                             selected["bw"],
                             this.avgSessionSecs);
    debug(id + ": attachedStream created");
  }
  
  
  
  getBestQuality(id, qualityList) {
    let highest_seen = qualityList[0];
    
    qualityList.forEach(function(q) {
      debug(id + ": Looking for highest quality: " + q.bw);
      if (q.bw > highest_seen.bw) {
        debug(id + ": New best quality: " + q.bw);
        highest_seen = q;
      }
    });
    
    debug(id + ": getBestQuality, best quality is: " + highest_seen.bw);
    return highest_seen;
  }
}



module.exports = StreamManager;
