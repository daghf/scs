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
 *
 *
 * Simulate multiple concurrent HLS stream clients.
 *
 * OPERATION:
 *
 * Start up a number of concurrent HLS clients, each of which will:
 * 
 *   Fetch quality list
 *   
 *   Pick a quality (at random, best available or skewed towards best)
 *   
 *   Fetch index file for the selected quality
 *   
 *   Figure out time span of segments
 *   
 *   Decide where to start playing, fetch two segments immediately
 *   
 *   Periodically refetch index file to get new segments if live
 *   stream
 *   
 *   Maintain a simulated buffer (metadata only) and pretend to play
 *   the stream, check that the next segment is available when
 *   required
 *
 * Detect when clients terminate, start up new ones to keep the number
 * of running clients constant.
 * 
 * Or try to dynamically figure out how many clients the server can
 * handle, without overwhelming it
 *
 * Or try to overwhelm the server by slowly adding clients
 *
 * When reading URLs from a file, if the URL is complete, SCS will use
 * it as is. If the url is just a path, SCS will try to use the stream
 * host and protocol specified on the command line. Protocol defaults
 * to https.
 *
 */

"use strict";

global.verbose = false;

require('./scs-helpers.js');

const Cooloff        = require('./cooloff.js');
const StatsCollector = require('./statscollector.js');
const StreamManager  = require('./streammanager.js');

const fs           = require('fs');
const util         = require('util');

let defaults = {   // Possibly overridden by cmd line params
  "avgSessionSecs"     : 60*60*24,
  "bindIp"             : "0.0.0.0",
  "bw"                 : "random",   // Select random quality by default
  "dynamicMaxLoad"     : false,
  "hostHeader"         : "",
  "ignoreCertErrors"   : false,
  "immediateFactor"    : 0,
  "killFactor"         : 25,
  "maxStreams"         : 3,
  "pesterSlowServer"   : false,
  "protocol"           : "https",
  "randomStartpoint"   : false,
  "skipAudioOnly"      : false,
  "skipUrls"           : 0,
  "softStart"          : false,
  "startupDelay"       : 0,
  "useUtilizationFile" : false,
  "utilizationMax"     : 100
}
let paramMap = {
  "b" : "bindIp",
  "d" : "startupDelay",
  "f" : "urlFile",
  "h" : "streamHost",
  "H" : "hostHeader",
  "i" : "ignoreCertErrors",
  "k" : "killFactor",
  "n" : "maxStreams",
  "p" : "protocol",
  "s" : "avgSessionSecs",
  "u" : "useUtilizationFile",
  "dynamic"            : "dynamicMaxLoad",
  "immediate"          : "immediateFactor",
  "influx-tag"         : "influxTag",
  "pester-slow-server" : "pesterSlowServer",
  "random-startpoint"  : "randomStartpoint",
  "resume-state"       : "resumeState",
  "skip-audio-only"    : "skipAudioOnly",
  "skip-urls"          : "skipUrls",
  "soft-start"         : "softStart",
  "stream-url"         : "streamUrl",
  "stream-path"        : "streamPath",
  "umax"               : "utilizationMax"
};



function printHelp() {
  let cmd = process.argv[1];
  console.log("Usage: nodejs "
              + cmd.substring(cmd.lastIndexOf("/") + 1)
              + " <options>");
  let text = `
Simulate one or more HLS stream clients

Options:
-b  IP               Bind to IP, connect from specific local IP
-d  SECS             Delay each client startup by a random time averaging SECS
                       seconds (to spread clients out when starting a run)
-f  FILE             Read URLs from FILE
-h  HOST             Fetch streams from HOST
-H  HOST             Set Host: header to HOST
-i                   Ignore SSL certificate problems
-k  NUM              % of clients to kill when server is overloaded. Default 25.
-n  NUM              Start up to NUM clients running in parallel. If --dynamic
                       is also set, ramp quickly up to NUM clients, then keep
                       ramping slowly as long as all clients are running well
-p  PROTO            Use protocol PROTO (http or https)
-s  SECS             Run each session for an average of SECS seconds. Default is
                       24 hours.
-u                   Use /utilization.json on the backend server to limit
                       traffic. See README.md for more info.
-v                   Log verbosely for debugging

--bw best            Select the best available quality
--bw biased          Select random quality, but biased towards the best one
--dynamic            Dynamically approach max possible load
--immediate NUM      % of clients to start immediately
--influx-tag TAG     Send statistics to Influx with TAG (e.g. node id)
--pester-slow-server Do not quit when buffer is empty and server is unresponsive
--random-startpoint  Skip to random point in stream
--resume-state FILE  Write state to/read state from FILE to resume a run.
  --skip-audio-only    Ignore audio-only streams
--skip-urls NUM      Skip first NUM urls (typically to resume a run, can be
                       used together with --resume-state)
--soft-start         Start ramping extra slowly
--umax NUM           Stop spawning whenever utilization is above NUM (see -u)
--stream-url URL     Use provided proto, host, path instead of reading from file
`;
  console.log(text);
}



function stripDashes(s) {
  let match = s.match(/^-+(.*)/);
  if (match) {
    return match[1];
  }
  return s;
}



function mapParam(p) {
  return paramMap[p] || p;
}



function parseArgs() {
  let args = new Object;
  let i = 0;
  let key;
  let match;
  let boolFound;
  let booleans = [
    "-i",
    "-u",
    "--dynamic",
    "--pester-slow-server",
    "--random-startpoint",
    "--skip-radio",
    "--soft-start"
  ];
  
  for (let a of process.argv.slice(2)) {
    log("i: " + i + ", a: " + a);
    // Special params
    if (a === "--help") {
      printHelp();
      process.exit(0);
    }
    if (a === "-v") {
      global.verbose = true; // Global
      continue;
    }
    
    // Flags
    for (let b of booleans) {
      let boolFound = false;
      if (a === b) {
        boolFound = true;
        debug("Found bool: " + a);
        args[mapParam(stripDashes(a))] = true;
        break;
      }
    }
    if (boolFound) {
      continue;
    }
    
    // Flags processed, the rest should have args, i.e. --foo bar
    if (++i % 2 === 1) {
      let match;
      log("key: " + a);
      if (! a.match(/^-/)) { // args start with - or --
        log("Bad argument key: " + a);
        process.exit(1);
      }
      key = stripDashes(a);
    } else {
      if (match = a.match(/^-/)) { // arg values do not start with -
        log("Bad argument value: " + a);
        process.exit(1);
      }
      key = mapParam(key);
      args[key] = a;
      log("val: " + args[key]);
    }
  }
  
  return args;
}



function parseStreamUrl() {
  let match = args.streamUrl.match(/^(http.*):\/\/([^\/]+)(\/.*)/);
  if (match) {
    args["protocol"] = match[1];
    args["streamHost"] = match[2];
    args["streamPath"] = match[3];
  }
}



//
// Main program
//

let args = parseArgs();

log("args: " + jsonpretty(args));

if (args["streamUrl"]) {
  parseStreamUrl();
}

if (! args["streamHost"]) {
  printHelp();
  console.log("No -h parameter, exiting");
  process.exit(1);
}

for (let def of Object.keys(defaults)) {
  args[def] = args[def] || defaults[def];
}

args["utilizationMax"]  = parsePctArg(args, "utilizationMax");
args["killFactor"]      = parsePctArg(args, "killFactor") / 100;
args["immediateFactor"] = parsePctArg(args, "immediateFactor") / 100;

args["skipUrls"]       = parseIntArg(args, "skipUrls");
args["startupDelay"]   = parseIntArg(args, "startupDelay");
args["maxStreams"]     = parseIntArg(args, "maxStreams");
args["avgSessionSecs"] = parseIntArg(args, "avgSessionSecs");

log("args: " + jsonpretty(args));

let urlFile = args["urlFile"] || "";

console.log("skipUrls: " + args["skipUrls"]);
if (args["resumeState"]) {
  let resumeState;
  
  console.log("Found resume-state arg: " + args["resumeState"]);
  try {
    let resumedata = fs.readFileSync(args["resumeState"]);
    resumeState = parseInt(resumedata, 10);
    if (isNaN(resumeState)) {
      console.log("Can not find resume data in " + args["resumeState"]);
      resumeState = 0;
    }
  } catch(e) {
    console.log("Unable to read resume state file, starting at beginning");
    resumeState = 0;
  }
  
  console.log("Resume state: " + resumeState);
  args["skipUrls"] += resumeState;
  console.log("Skip urls: " + args["skipUrls"]);
}

let streamManager = new StreamManager(args);

if (urlFile) {
  streamManager.runUrlFile(urlFile);
} else {
  streamManager.startFixedNumberOfClients();
}
