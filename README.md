# This is a simulator for HLS streaming clients

## Installation for development:
* Install node.js (e.g. sudo apt install nodejs)
* Install npm (e.g. sudo apt install npm)
* Clone the repository and cd into it
* Edit scs.conf (currently only needed for reporting to influx)
```
npm i
```


## Run SCS:
```
./scs <options>
```

## Run multiple instances of SCS:
* Read doc in scs-multi
* Edit scs-multirc
* Edit the scs command line in scs-multi
```
./scs-multi
```

## Run one or a few instances of SCS on each node in a cluster:
See scs-cluster

## /utilization.json

The -u option to SCS will cause it to read the file /utilization.json
from the server and use the "sys_utilization" property to decide
whether to spawn new clients or not, in combination with the --umax
option. For this to work, a process on the server must map server
load, network utilization etc to a percentage and keep the
utilization.json file updated. The following is sufficient:

```
{
"sys_utilization": 93
}
```

## Kill all running scs processes
```
pkill -f scs.js
```

## Examples

Run two clients immediately, both reading the same stream URL
```
./scs -n 2 --stream-url 'https://vcache-vod.example.com/some/stream/path/playlist.m3u8?foo=10&bar=20'
```

Identical result as the command above:
```
./scs -n 2 -h vcache-vod.example.com -p https --stream-path '/some/stream/path/playlist.m3u8?foo=10&bar=20'
```

Run two stream clients, start both immediately, attach to vcache-vod.example.com using default protocol (https), read paths from /tmp/streams.txt:
```
./scs -n 2 -h vcache-vod.example.com -f /tmp/streams.txt
```
Note that the lines in /tmp/streams.txt should be paths, not complete URLs. If they are complete URLs, SCS will take host and protocol from the URLs and ignore the command line parameters.

Run 50 clients, start them with a random delay of up to 10 seconds, otherwise as above:

```
./scs -n 50 -d 10 -h vcache-vod.example.com -f /tmp/streams.txt
```

Start 10 % of clients immediately, ramp quickly to 200 clients, keep trying to add more clients as long as server still seems ok

```
./scs --immediate 10 -n 200 --dynamic -h vcache-vod.example.com -f /tmp/streams.txt
```
