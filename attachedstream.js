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

//
// Class for playing streams
//
class AttachedStream {
  constructor(streamData, manager, url, bw, avgSessionSecs) {
    // Fill the buffer and start playing. Then start refreshing
    // the buffer as the "player" empties it.  super();
    let theStream = this;
    
    debug(streamData.id + ": AttachedStream constructor running");
    debug(streamData.id + ": streamData: " + jsondebug(streamData));
    
    this.id             = streamData.id;
    this.lineNumber     = streamData.lineNumber;
    this.manager        = manager;
    this.streamUrl      = url;
    this.bandwidth      = bw;
    this.avgSessionSecs = avgSessionSecs;
    this.bufferSize     = 5;
    this.maxRetries     = 10;
    this.baseUrl        = streamData.baseUrl;
    this.isLive         = undefined;
    
    this.segments               = [];
    this.bufferedSegments       = [];
    this.requestedSegments      = {};
    this.activeRequests         = {};
    this.activeRequestNumber    = 0;
    this.lastPlayedSegment      = undefined;
    this.nextSeqnoToPlay        = -1;
    this.numberOfPlayedSegments = 0;
    this.nBufferWaits           = 0;
    this.accumulatedStreamTime  = 0;
    this.isAttached             = 0;
    this.isReattaching          = 0;
    this.isTerminating          = 0;
    this.isTerminated           = 0;
    this.terminationTimeout     = undefined;
    // this.terminationTime        = 15000;
    
    this.https_agent = initHttpsAgent();
    this.http_agent = initHttpAgent();
    
    this.attachCallback = function() {
      debug(theStream.id + ": emitting streamClientAttached event");
      theStream.manager.emit('streamClientAttached', theStream);
      theStream.attach();
    }
    
    this.refreshCallback = function() {
      theStream.refresh();
    }
    
    this.fetchSegmentData(this.attachCallback);
  }
  
  
  
  attach() {
    debug(this.id + ": attach()");
    
    let theStream = this;
    
    this.isAttached = 1;
    
    this.terminationTimeout = setTimeout(function() {
      theStream.terminate();
    }, theStream.terminationTime());
    
    setTimeout(function() {
      theStream.fetchSegmentData(theStream.refreshCallback);
    }, this.segmentDuration);
    
    if (this.manager.randomStartpoint) {
      this.startBufferingAtRandomPoint();
    } else if (this.isLive) {
      debug(this.id + ": Live stream, buffering at end");
      this.startBufferingAtEnd();
    } else {
      debug(this.id + ": On demand stream, buffering at beginning");
      this.startBufferingAtBeginning();
    }
    
    log(this.id + ": Successfully attached to " + this.streamUrl);
    
    this.play();
  }
  
  
  
  refresh() {
    debug(this.id + ": refresh()");
    
    if (this.isReattaching || this.isTerminating) {
      debug(this.id +
            ": Reattach or termination in progress, stopping refresh");
      return;
    }
    
    this.refillBuffer();
    
    let theStream = this;
    
    setTimeout(function() {
      if (theStream.isLive) { // Only live needs updated segment list
        debug(theStream.id + ": refresh(), live, updating segments");
        theStream.fetchSegmentData(theStream.refreshCallback);
      } else {
        debug(theStream.id + ": refresh(), vod, not updating segments");
        theStream.refreshCallback(theStream);
      }
    }, this.segmentDuration);
  }
  
  
  
  reattach() {
    log(this.id + ": reattach()");
    
    this.isAttached = 0;
    this.isReattaching = 1;
    
    // Wait for activity to finish, then attach again
    let theStream = this;
    setTimeout(function() {
      theStream.finishReattach();
    }, theStream.segmentDuration);
  }
  
  
  
  finishReattach() {
    debug(this.id + ": finishReattach()");
    
    let theStream = this;
    let finishReattachCallback = function () {
      // Reset counters
      theStream.bufferedSegments       = [];
      theStream.lastPlayedSegment      = undefined;
      theStream.nextSeqnoToPlay        = -1;
      theStream.numberOfPlayedSegments = 0;
      theStream.accumulatedStreamTime  = 0;
      
      theStream.isReattaching = 0;
      
      debug(theStream.id,
            ": finishReattachCallback, counters cleared, attaching...");
      
      theStream.attach();
    }
    
    this.fetchSegmentData(finishReattachCallback);
  }
  
  
  
  terminate() {
    // We have been running, now we're done or experienced an
    // unrecoverable error
    log(this.id + ": terminate()");
    this.doTerminate();
    this.manager.emit('streamClientTerminated', this);
  }
  
  
  
  doTerminate() {
    if (! this.isTerminating) {
      this.isTerminating = 1;
      clearTimeout(this.terminationTimeout); // Plug memory leak
      this.abortAllRequests();
    } else {
      log(this.id + ": Already terminated, no action");
    }
  }
  
  
  
  abort() {
    // We got an unrecoverable error before we had the chance to start
    log(this.id + ": abort()");
    this.isTerminating = 1;
    this.manager.emit('streamClientAborted', this.id);
  }
  
  
  
  kill() {
    // Deliberately killed by manager.
    log(this.id + ": kill()");
    this.doTerminate();
    this.manager.emit('streamClientKilled', this);
  }
  
  
  
  abortOrTerminate() {
    log(this.id + ": abortOrTerminate()");
    if (this.isAttached) {
      this.terminate();
    } else {
      this.abort();
    }
  }
  
  
  
  redirected() {
    log(this.id + ": redirected()");
    this.isTerminating = 1; // Stop play and refresh
    this.manager.emit('streamClientRedirected', this);
  }
  
  
  abortAllRequests() {
    for (let key in this.activeRequests) {
      log(this.id + ": Aborting request for " + key);
      this.activeRequests[key].abort();
    }
  }
  
  
  
  terminationTime() {
    // Simple calculation to get a random run time distribution
    // with an average runtime equal to this.avgSessionSecs.
    //
    // This is a gross simplification and may well be too far away
    // from reality.
    //
    // If the average is 10 minutes, we want anywhere from 0 to 20 minutes
    let avgSecs = this.avgSessionSecs;
    
    debug(this.id + ": terminationTime(), avgSessionSecs: " + avgSecs);
    let retval = (Math.random() * avgSecs * 2) * 1000; // to ms;
    debug(this.id, ": terminationTime() returning", retval.toFixed(3), " ms");
    
    return retval;
  }
  
  
  
  sendRequest(url, callback, discardBody) {
    let theStream = this;
    
    let options = {
      https_agent: this.https_agent,
      http_agent : this.http_agent
    };
    
    if (this.hostHeader) {
      options.headers = {
        Host: this.hostHeader
      };
    }
    
    let requestNumber = this.activeRequestNumber++;
    
    let wrappedCallback = function(err, res, body) {
      callback(err, res, body);
      delete theStream.activeRequests[requestNumber];
    };
    
    
    let thisRequest = doSendRequest(url, options, wrappedCallback, discardBody);
    
    this.activeRequests[requestNumber] = thisRequest;
  }
  
  
  
  // Fetch segment data, send them to parsing when they arrive.
  fetchSegmentData(action) {
    let theStream = this;
    
    let responseCallback = function(err, res, body) {
      let retry = 0;
      
      if (! res) {
        if (err) {
          log(theStream.id
              + ': fetchSegmentData: Request failed ('
              + theStream.streamUrl + "): " + err.message);
          retry = 1;
        } else {
          log(theStream.id + ": No result and no error??")
          return;
        }
      } else if (res.statusCode === 200) {
        try {
          theStream.maxRetries = 10; // Success, reset retries counter
          theStream.parseSegmentData(body, action);
        } catch (err) {
          log(theStream.id + ": parseSegmentData failed: " + err);
          theStream.abortOrTerminate();
        }
      } else if (isRedirect(res.statusCode)) {
        log(theStream.id + ": HTTP REDIRECT: " + res.statusCode +
            ", url: " + theStream.streamUrl);
        theStream.redirected();
        return;
      } else if (res.statusCode >= 400 &&
                 res.statusCode <  500) {
        log(theStream.id + ": fetchSegmentData: HTTP "
            + res.statusCode + ", abortOrTerminate");
        theStream.abortOrTerminate();
      }
      else {
        log(theStream.id + ": fetchSegmentData: HTTP NOT OK: " +
            res.statusCode + ", url: " + theStream.streamUrl);
        retry = 1;
      }
      
      if (retry) {
        if (theStream.maxRetries-- <= 0) {
          log(theStream.id + ": Max retries exceeded. abortOrTerminate");
          theStream.abortOrTerminate();
          return;
        }
        let timeout = theStream.segmentDuration
            ? theStream.segmentDuration
            : 10000;
        
        log(theStream.id +
            ": fetchSegmentData: Request failed, retrying in " +
            timeout/1000 + " s");
        debug(theStream.id + ": Retries left: " + theStream.maxRetries);
        
        setTimeout(function() {
          theStream.fetchSegmentData(action);
        }, timeout);
      }
      
    };
    
    if (action === this.refreshCallback) {
      debug(this.id + ": fetchSegmentData running, next action is refresh");
    }
    else if (action === this.attachCallback) {
      debug(this.id + ": fetchSegmentData running, next action: attach");
    }
    else {
      debug(this.id + ": fetchSegmentData running, next action: finishReattach");
    }
    
    debug(this.id + ": fetchSegmentData: streamUrl: " + this.streamUrl);
    this.sendRequest(this.streamUrl, responseCallback);
  }
  
  
  
  // Wait for data if necessary, play a segment when
  // available. Reschedule for another run when the segment is done.
  play() {
    let theStream = this;
    
    debug(this.id + ": play()");
    
    this.waitForBuffering(function () {
      theStream.playASegment();
    });
  }
  
  
  
  // Download a single segment, insert it into the buffer.
  // 
  // If there's any buffered segments, play one of them. Otherwise,
  // wait a bit and try again. This continues until segments are
  // available.
  waitForBuffering(playCallback) {
    debug(this.id + ": waitForBuffering()");
    
    if (this.isTerminating) {
      log(this.id + ": waitForBuffering(): Termination in progress, " +
          "stopping play");
      return;
    }
    
    // Check that we have received the first/next segment before
    // playing. startBuffering records the first sequence number
    // in the stream so we know what to look for when starting.
    debug(this.id + ": bufferedSegments[0]: " +
          jsondebug(this.bufferedSegments[0]))
    debug(this.id + ": nextSeqnoToPlay: " + this.nextSeqnoToPlay)
    if (this.bufferedSegments[0]) {
      debug(this.id + ": bufferedSegments[0].seq: " +
            this.bufferedSegments[0].seq);
    }
    
    let lastSegment = this.segments[this.segments.length - 1];
    
    if (this.bufferedSegments[0]  &&
        this.nextSeqnoToPlay >= 0 && 
        this.nextSeqnoToPlay === this.bufferedSegments[0].seq) {
      this.nBufferWaits = 0;
      debug(this.id + ": Data ready for playing");
      playCallback();
    } else if (! this.isLive && lastSegment &&
               lastSegment.seq <= this.nextSeqnoToPlay) {
      // At end of VOD stream
      debug(this.id + ": lastSegment.seq: " + lastSegment.seq);
      debug(this.id + ": nextSeqnoToPlay: " + this.nextSeqnoToPlay);
      debug(this.id + ": At end of VOD stream, terminating");
      this.terminate();
    } else {
      if (! this.isLive && ! lastSegment) {
        log(this.id + ": ERROR: OD stream, lastSegment undef in " +
            "waitForBuffering, what's this?");
        log(this.id + ": segments.length: " + this.segments.length);
        log(this.id + ": segments: " + JSON.stringify(this.segments));
        log(this.id + ": numberOfPlayedSegments: " +
            this.numberOfPlayedSegments);
      }
      debug(this.id + ": No data ready for playing, waiting...");
      let waitsPerSegment = 5;
      let waitInterval = this.segmentDuration / waitsPerSegment;
      
      if (! this.numberOfPlayedSegments) {
        // Just started up, first segment hasn't arrived yet
        debug(this.id + ": Buffering...");
      } else {
        // Buffer underrun during play, bad
        this.nBufferWaits++;
        if (this.nBufferWaits * waitInterval > this.segmentDuration) {
          // Waited more than a whole segment duration.
          debug(this.id + ": Very bad service in waitForBuffering()");
          this.handleVeryBadService();
        } else {
          debug(this.id + ": Bad service in waitForBuffering()");
          this.handleBadService();
        }
      }
      this.logBufferedSegments("waitForBuffering()");
      debug(this.id + ": Waiting for " + this.nextSeqnoToPlay);
      debug(this.id + ": Buffering... waitInterval: " + waitInterval);
      
      let theStream = this;
      setTimeout(function() {
        theStream.waitForBuffering(playCallback);
      }, waitInterval);
    }
  }
  
  
  
  // Remove a segment from the buffer and register it as the last
  // played one.
  playASegment() {
    let theStream = this;
    let segment;
    let wallclockTime;
    
    this.logBufferedSegments("playASegment()");
    
    segment = this.bufferedSegments.shift();
    this.removeOldSegmentRequests(segment);
    
    debug(this.id + ": Playing segment: " + segment.seq +
          " of duration: " + segment.duration);
    
    if (this.numberOfPlayedSegments === 0) {
      this.startTime = (new Date).getTime();
      debug(this.id + ": Start time set: " + this.startTime);
    }
    
    wallclockTime = (new Date).getTime() - this.startTime;
    this.lastPlayedSegment = segment;
    this.nextSeqnoToPlay++;
    this.numberOfPlayedSegments++;
    this.lag = wallclockTime - this.accumulatedStreamTime;
    
    debug(this.id
          + ": Stream time: "
          + (this.accumulatedStreamTime / 1000).toFixed(3)
          + ", clock time: "  + (wallclockTime / 1000).toFixed(3));
    debug(this.id
          + ": Lagging " + (this.lag / 1000).toFixed(3)
          + " seconds");
    
    // Try to catch up any lag
    let playNextAt = segment.duration - this.lag;
    
    if (playNextAt < 0) {
      // Should have played this already, play now. Effectively
      // like skipping over missing segments Not necessary to
      // report bad service, waitForBuffering has already done
      // that.
      playNextAt = 0;
    }
    
    setTimeout(function() {
      theStream.play();
    }, playNextAt);
    if (segment.httpStatus !== 200) {
      log(this.id
          + ": Segment " + segment.seq
          + " failed to download with status code "
          + segment.httpStatus + ", skipping");
      this.terminateIfAllSegmentsFailed();
    } else {
      debug(this.id
            + ": Playing segment number "
            + this.numberOfPlayedSegments + ": "
            + jsondebug(segment));
    }
    debug(this.id
          + ": Set up callback to play next segment in "
          + (playNextAt / 1000).toFixed(3) + " seconds");
    
    this.accumulatedStreamTime += segment.duration;
  }
  
  
  
  handleBadService() {
    log(this.id + ": BAD SERVICE DETECTED");
    this.manager.emit('streamClientBadService', this);
  }
  
  
  
  handleVeryBadService() {
    log(this.id + ": VERY BAD SERVICE DETECTED");
    this.manager.emit('streamClientVeryBadService', this);
  }
  
  
  
  handleSlowServer() {
    log(this.id + ": SLOW SERVER DETECTED");
    this.manager.emit('streamClientSlowServer', this);
  }
  
  
  
  terminateIfAllSegmentsFailed() {
    let anyGood = 0;
    
    if (this.manager.pesterSlowServer) {
      debug("pesterSlowServer is true, not terminating");
      return;
    }
    
    this.bufferedSegments.forEach(function (s) {
      if (s.httpStatus === 200) {
        anyGood = 1;
      }
    });
    
    if (! anyGood) {
      log(this.id + ": All segments in buffer failed to download. Terminating.");
      log(this.id + ": bufferedSegments: "
          + JSON.stringify(this.bufferedSegments));
      this.abortOrTerminate();
    }
  }
  
  
  
  // Parse the file containing the urls of the video segments,
  // trigger further processing once parsing is done. This can be
  // either to attach to the stream or to fill more segments into
  // the buffer.
  parseSegmentData(segmentData, action) {
    let newSegments = [];
    let segmentDuration = 1000; // short duration by default, in ms 
    // let firstMediaSeqno = 0;
    // let segmentCount    = 0;
    let currentSegmentNumber = 0;
    let theStream = this;
    let nextSegmentDuration = 0;
    
    segmentData.split("\n").forEach(function(s) { 
      let match;
      let unplayedSegment =
          (theStream.nextSeqnoToPlay < 0
           || (currentSegmentNumber >= theStream.nextSeqnoToPlay));
      
      if (s[0] !== '#' && s.match(/\w+/)) {
        // Skip segments we don't need - i.e. segments earlier
        // than the one we're going to play next
        if (unplayedSegment) {
          newSegments.push({"seq"      : currentSegmentNumber,
                            "url"      : s,
                            "duration" : nextSegmentDuration});
        }
        currentSegmentNumber++;
      }
      // Duration of the segment in the next line
      else if (unplayedSegment &&
               (match = s.match(/^#EXTINF:(\d+\.\d+)/))) {
        nextSegmentDuration = parseFloat(match[1]) * 1000; // to ms
      }
      // Metadata
      else if (! s.match(/^#EXTM|^#EXTI/)) {
        if (match = s.match(/^#EXT-X-PLAYLIST-TYPE:(\w+)/)) {
          if (theStream.isLive === undefined) {
            theStream.setStreamType(match[1]);
          }
        }
        
        // Maximum segment duration
        if (match = s.match(/^#EXT-X-TARGETDURATION:(\d+)/)) {
          segmentDuration = match[1];
          segmentDuration *= 1000; // Convert to ms
          debug(theStream.id + ": Segment duration: " + segmentDuration);
        }
        // First sequence number of this segment batch
        else if (match = s.match(/^#EXT-X-MEDIA-SEQUENCE:(\d+)/)) {
          currentSegmentNumber = parseInt(match[1], 10);
          debug(theStream.id + ": First segment sequence number: " +
                currentSegmentNumber);
          
        }
        // Live or VOD?
        else if (match = s.match(/^#EXT-X-PLAYLIST-TYPE:VOD/)) {
          debug(theStream.id + ": VOD stream");
          if (theStream.isLive) {
            log(theStream.id + ": ERROR: URL heuristics for Live/VOD failed!");
            theStream.abortOrTerminate();
          }
        }
      }
    });
    
    this.segmentDuration = segmentDuration;
    
    // discard unneeded segments unless starting up
    if (this.isLive && this.nextSeqnoToPlay >= 0) {
      // debug(this.id + ": parseSegmentData, newSegments: "
      //       + jsondebug(newSegments));
      let end = Math.min(this.bufferSize, newSegments.length);
      // debug(this.id + ": parseSegmentData, end: " + end);
      this.segments = newSegments.slice(0, end);
      // debug(this.id + ": parseSegmentData, segments after crop: "
      //      + jsondebug(this.segments));
    } else {
      this.segments = newSegments;
    }
    
    // debug(this.id + ": parseSegmentData, this.segments: "
    //       + jsondebug(this.segments));
    
    action(this);
  }
  
  
  
  setStreamType(streamType) {
    // See rfc8216
    debug(this.id + ": Playlist type: " + streamType);
    if (streamType === "VOD" || streamType === "EVENT") {
      this.isLive = false;
    } else {
      this.isLive = true;
    }
  }
  
  
  
  fetchSegment(segment) {
    let theStream = this;
    let url = this.baseUrl + "/" + segment.url;
    
    let segmentCallback = function(err, res, body) {
      if (err) {
        log(theStream.id + ': Request failed (' + segment.url + "): "
            + err.message);
        return;
      }
      
      if (res.statusCode === 200) {
        // debug(theStream.id + ": Done fetching " +
        //       Math.round(body.length / 1024) +
        //       " kB from " + url);
        debug(theStream.id + ": Done fetching segment from " + url);
      } else if (isRedirect(res.statusCode)) {
        log(theStream.id + ": Segment fetch was redirected.")
        res.statusCode = 200; // Assume redirect target delivered it
      } else {
        log(theStream.id + ": segment " + segment.seq +
            ": HTTP NOT OK: " + res.statusCode + ", url: " + url)
      }
      theStream.insertIntoBuffer(segment, res.statusCode);
      theStream.logBufferedSegments("fetchSegment()");
    };
    
    // Check whether it's in the buffer. Another fetch or refetch
    // may have inserted it already. Have to check again after the
    // request terminates too.
    for (let i = 0; i < this.bufferedSegments.length; i++) {
      if (segment.seq === this.bufferedSegments[i].seq) {
        debug(this.id + ": Buffer already contains " + segment.seq
              + ", not fetching");
        return;
      }
    }
    
    debug(this.id + ": Fetching segment number " + segment.seq);
    debug(this.id + ": Segment number " + segment.seq + " is " + url);
    
    this.registerSegmentRequest(segment);
    
    let slowness = this.checkForSlowServer(segment);
    if (slowness && ! this.manager.pesterSlowServer) {
      // Don't request missing segments again, that causes load
      // at the already overloaded server to go through the
      // roof.
      this.handleSlowServer();
    } else if (slowness && this.manager.pesterSlowServer) {
      debug(this.id + ": Re-requesting segment number " + segment.seq);
      this.handleSlowServer();
      let discardBody = true;
      this.sendRequest(url, segmentCallback, discardBody);
    } else {
      let discardBody = true;
      this.sendRequest(url, segmentCallback, discardBody);
    }
  }
  
  
  
  registerSegmentRequest(segment) {
    if (segment.seq in this.requestedSegments) {
      this.requestedSegments[segment.seq]++;
    } else {
      this.requestedSegments[segment.seq] = 1;
    }
  }
  
  
  
  checkForSlowServer(segment) {
    if (segment.seq in this.requestedSegments) {
      if (this.requestedSegments[segment.seq] > 1) {
        // This one has been requested before, server is slow
        log(this.id + ": Segment " + segment.seq + " was requested " +
            this.requestedSegments[segment.seq] + " periods ago.");
        return 1;
      }
    }
    return 0;
  }
  
  
  
  removeOldSegmentRequests(segment) {
    for(let seq in this.requestedSegments) {
      if (seq < segment.seq) {
        delete this.requestedSegments[seq];
      }
    };
    
    // log(this.id + ": requestedSegments: " +
    //     JSON.stringify(this.requestedSegments));
  }
  
  
  
  // Insert segment metadata into the buffer, sorted on sequence
  // number. The actual data is discarded, we just count the bytes.
  insertIntoBuffer(segment, httpStatus) {
    // Abort if the stream is reattaching
    if (this.isReattaching) {
      debug(this.id + ": Reattach in progress, not inserting " + segment.seq);
      return;
    }
    
    // Already past it, so drop it
    if (segment.seq < this.nextSeqnoToPlay) {
      log(this.id + ": " + segment.seq + " arrived late, not inserting");
      return;
    }
    
    // No duplicates
    for (let i = 0; i < this.bufferedSegments.length; i++) {
      if (segment.seq === this.bufferedSegments[i].seq) {
        log(this.id + ": Buffer already contains " + segment.seq
            + ", not inserting");
        return;
      }
    }
    
    segment.httpStatus = httpStatus;
    this.bufferedSegments.push(segment);
    this.bufferedSegments.sort(function(a, b) {
      return a.seq - b.seq;
    });
  }
  
  
  
  // Format debugging info on the contents of the buffer.
  logBufferedSegments(msg) {
    debug(this.id + ": " + msg + ", buffered segments: " +
          this.logSegmentList(this.bufferedSegments));
    
  }
  
  
  
  logSegmentList(segments) {
    let str = "["
    
    if (segments.length > 0) {
      segments.forEach(function(s) {
        str += s.seq + ", ";
      });
      str = str.slice(0, -2);
    }
    
    return str + "]";
  }
  
  
  
  // VOD streams start at the beginning
  startBufferingAtBeginning() {
    debug(this.id + ": startBuffering fetching the first two segments.");
    this.startBuffering(this.segments.slice(0, 3));
  }
  
  
  
  // Live streams start near the end.
  startBufferingAtEnd() {
    debug(this.id + ": startBuffering fetching the first two segments.");
    this.startBuffering(this.segments.slice(-this.bufferSize));
  }
  
  
  
  // Pick a random point somewhere in the live stream buffer
  startBufferingAtRandomPoint() {
    let nSegments     = this.segments.length;
    let maxStartPoint = nSegments - this.bufferSize;
    let startPoint    = getRandomInt(0, maxStartPoint);
    
    let pct = startPoint / maxStartPoint * 100;
    log(this.id + ": Start buffering at " + pct + " %.")
    
    let segmentsToBuffer = this.segments.slice(startPoint, startPoint + 2);
    this.startBuffering(segmentsToBuffer);
  }
  
  
  
  startBuffering(segmentsToBuffer) {
    this.nextSeqnoToPlay = segmentsToBuffer[0].seq;
    log(this.id + ": nextSeqnoToPlay: " + this.nextSeqnoToPlay);
    
    // Request the first segment and the one following it
    this.fetchSegment(segmentsToBuffer[0]);
    this.fetchSegment(segmentsToBuffer[1]);
  }
  
  
  
  // Figure out how many more segments the buffer has space
  // for. Download and buffer that many new segments. Stop when the
  // buffer is full or when there are no more new segments to
  // download.
  refillBuffer() {
    let theStream = this;
    
    this.logBufferedSegments("refillBuffer()");
    
    let missingSegments = this.findMissingSegments();
    log(this.id + ": missingSegments: " + this.logSegmentList(missingSegments));
    let count = 0;
    missingSegments.forEach(function(s) {
      if (++count > 2) {
        // Fetch max 2 segments at a time, this means that a
        // newly started stream has 2x the data rate of a
        // stable one.
        return;
      }
      theStream.fetchSegment(s);
    });
  }
  
  
  
  findMissingSegments() {
    // When starting up, startBuffering will set nextSeqnoToPlay
    // based on the segment data and buffer size. No buffer
    // manipulation happens before this, so nextSeqnoToPlay is always
    // safe to use.
    let nextSeqno       = this.nextSeqnoToPlay;
    let buf             = this.bufferedSegments;
    let missingSegments = [];
    let missingSeqnos   = [];
    
    // Initialize missingSeqnos with all sequence numbers that could
    // possibly be missing - that is, those that are equal to or
    // greater than the next sequence number to play and not more than
    // bufferSize larger.
    for (let i = 0; i < this.bufferSize; i++) {
      missingSeqnos[i] = nextSeqno + i;
    }
    // debug(this.id + ": missing sequence numbers (init): "
    //       + jsondebug(missingSeqnos));
    
    // Go through missingSeqnos again and set entries that we already
    // have to 0. The index into the missingSeqnos array can be
    // calculated from the sequence numbers of the buffered segment
    // and the last segment played.
    for (let i = 0; i < this.bufferSize; i++) {
      if (buf[i] &&
          buf[i].seq >= nextSeqno &&
          buf[i].seq < nextSeqno + this.bufferSize) {
        let ix = buf[i].seq - nextSeqno;
        missingSeqnos[ix] = 0;
      }
    }
    // debug(this.id + ": missing sequence numbers: "
    //       + jsondebug(missingSeqnos));
    // debug(this.id + ": segments: " + jsondebug(this.segments));
    
    // Find index of the first missing sequence number in the segments array
    let i = 0;        
    let j = 0;
    while (i < missingSeqnos.length && j < this.segments.length) {
      debug(this.id + ": missingSeqnos[" + i + "]: " + missingSeqnos[i]);

      if (missingSeqnos[i] === 0) {
        // Skip those we already have
        i++;
        continue;
      }
      if (missingSeqnos[i] > this.segments[j]["seq"]) {
        // Looking for a later segment, skip
        j += (missingSeqnos[i] - this.segments[j]["seq"]);
        continue;
      }
      // When we get here, i and j point at slots belonging to
      // the same seqno
      if (missingSeqnos[i] === 0) {
        i++;
        j++;
        continue;
      }
      if (j < this.segments.length && this.segments[j]) {
        missingSegments.push(this.segments[j]);
      }
      i++;
      j++;
    }
    
    return missingSegments;
  }
  //
  // End of class AttachedStream
  //
}



module.exports = AttachedStream;
