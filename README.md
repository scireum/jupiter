![JUPITER](https://raw.githubusercontent.com/scireum/jupiter/master/docs/jupiter.png)

**Jupiter** is a framework for wrapping **compute** or **memory intense** components to
provide them as **high throughput** and **ultra low latency** services to
applications built on managed runtimes like **node.js**, **Java**, **Ruby**.

We at [scireum](https://www.scireum.de) use **Jupiter** in conjunction with our open source
Java framework [SIRIUS](https://github.com/scireum/sirius-kernel) to build web based
applications.

More infos and a detailed description can be found in the [Documentation](https://docs.rs/jupiter).

# Deployment

Deployment-wise we heavily settle on **Docker**, therefore we simply log to *stdout* and
use *SIGHUP* to detect a shutdown request. 

Although, Jupiter is intended to be used as library to build custom services, a standalone
docker image is provided under **Jupiter IO** - IO as in the moon of jupiter, not I/O operations :).

**Jupiter IO** has all modules (as listed above) enabled. Therefore you can have a go by calling:

```
> docker run -p 2410:2410 scireum/jupiter-io:latest &
> redis-cli -p 2410

> PING or
> SYS.COMMANDS
```

Note that the two volumes `/jupiter/config` and `/jupiter/repository` should
be mounted to docker volumes or external directories so that the data is kept
alive once the container is updated.

# Configuration

The configuration is loaded from **settings.yml** - modification of this file are detected and
distributed within the framework. Also, the application specific config can be pushed in by
sending **SYS.SET_CONFIG**.

A basic configuration would only specify the ip and port to bind to (if the default settings
aren't feasible):

```yaml
server:
    host: "0.0.0.0"
    port: 2410
```

## Repository

As a single repository might be shared by multiple applications which always sync all files into their
Jupiter server, files can be put into namespaces and the config can determine which namespaces are
enabled:

```yaml
repository:
    namespaces: ["core", "test", "foo" ]
```

## LRU Cache

Each cache can be configured to limit its size and control its lifetime parameters:
```yaml
caches:
    my_cache:
        # Specifies the maximal number of entries to store
        size: 1024
        # Specifies the maximal amount of memory to use (in bytes).
        # Supports common suffixes like: k, m, g, t
        max_memory: 1g
        # Specifies the soft time to live. After this period, an entry is considered stale
        # and will not be delivered by LRU.GET. However, LRU.XGET will deliver this entry
        # but mark it as stale. Supports common suffixes like: s, m, h, d
        soft_ttl: 15m
        # Specifies the hard time to live. After this persiod, neither LRU.GET nor LRU.XGET
        # will deliver this entry.
        hard_ttl: 1d
        # Specifies the refresh interval for LRU.XGET. If this command delivers a stale entry
        # (as defined by soft_ttl), it indicates that the entry is stale an should be
        # refreshed. However, once this has to be signalled to a client, it will no longer
        # request a refresh from other clients until either the entry has been refresehd or
        # this refresh interval has elapsed.
        refresh_interval: 30s
```

# Commands

If all modules are enabled, the following commands are available.

## Core Module
* `SYS.COMMANDS` lists all available commands.
* `SYS.CONNECTIONS` lists all active client connections.
* `SYS.KILL` terminates the connection to the client with the given ip.
*  `SYS.MEM` reports the current memory usage.

## Repository
* `REPO.SCAN` re-scans the local repository contents on the local disk. This
  automatically happens at startup and is only required if the contents of the repository are
  changed by an external process.
* `REPO.FETCH file url` instructs the background actor to fetch a file from the
  given url. Note that the file will only been fetched if it has been modified on the server
  since it was last fetched.
* `REPO.STORE file contents` stores the given string contents in a file.
* `REPO.FETCH_FORCED file url` also fetches the given file, but doesn't
  perform any "last modified" checks as `REPO.FETCH` would.
* `REPO.LIST` lists all files in the repository. Note that this will yield a
  more or less human readable output where as `REPO.LIST raw` will return an array with
  provides a child array per file containing **filename**, **filesize**, **last modified**.
* `REPO.DELETE file` deletes the given file from the repository.

## LRU Cache
* `LRU.PUT cache key value` will store the given value for the given key in the
  given cache.
* `LRU.GET cache key` will perform a lookup for the given key in the given cache
   and return the value being stored or an empty string if no value is present.
* `LRU.XGET cache key` will behave just like **LRU.GET**. However, its output is
   a bit more elaborate. It will always respond with three values: ACTIVE, REFRESH, VALUE. If
   no value was found for the given key, ACTIVE and REFRESH will be 0 and VALUE will be an empty
   string. If a non-stale entry way found, ACTIVE is 1, REFRESH is 0 an VALUE will be the value
   associated with the key. Now the interesting part: If a stale entry (older than *soft_ttl* but
   younger than *hard_ttl*) was found, ACTIVE will be 0. For the first client to request this
   entry, REFRESH will be 1 and the VALUE will be the stale value associated with the key. For
   all subsequent invocations of this command, REFRESH will be 0 until either the entry was
   updated (by calling **LRU.PUT**) or if the *refresh_interval* has elapsed since the first
   invocation. Using this approach one can build "lazy" caches, which refresh on demand, without
   slowing the requesting client down (stale content can be delivered quickly, if the application
   accepts doing so) and also without overloading the system, as only one client will typically
   try to obtain a fresh value instead of all clients at once.
* `LRU.REMOVE cache key` will remove the value associated with the given key.
   Note that the value will be immediately gone without respecting any TTL.
* `LRU.FLUSH cache` will wipe all contents of the given cache.
* `LRU.STATE` will provide an overview of all active cache. `LRU.STATE cache`
  will provide detailed metrics about the given cache.
  
## InfoGraphDB
* `IDB.LOOKUP table search_path filter_value path1 path2 path3`
  Performs a lookup for the given filter value in the given search path (inner fields separated
  by ".") within the given table. If a result is found, the values for path1..pathN are
  extracted and returned. If no path is given, the number of matches is returned. If multiple
  documents match, only the first one if returned.
* `IDB.ILOOKUP table primary_lang fallback_lang search_path filter_value path1`
  Behaves just like `IDB.LOOKUP`. However, of one of the given extraction paths points to an
  inner map, we expect this to be a map of translation where we first try to find the value for
  the primary language and if none is found for the fallback language.
* `IDB.QUERY table num_skip max_results search_path filter_value path1`
  Behaves just like lookup, but doesn't just return the first result, but skips over the first
  `num_skip` results and then outputs up to `max_result` rows. Not that this is again limited to
  at most **1000**.
* `IDB.QUERY table primary_lang fallback_lang num_skip max_results search_path filter_value path1`
  Provides essentially the same i18n lookups for `IDB.QUERY` as `IDB.ILOOKUP` does for
  `IDB.LOOKUP`.
* `IDB.SEARCH table num_skip max_results search_paths filter_value path1`
  Performs a search in all fields given as `search_paths`. This can either be comma separated
  like "path1,path2,path3" or a "*" to select all fields. Note that for a given search value,
  this will match case-insensitive and also for prefixes of a detected word within the
  document (the selected fields). Everything else behaves just like `IDB.QUERY`. Also note that
  a fulltext index has to be present for each field being queried.
* `IDB.ISEARCH table primary_lang fallback_lang num_skip max_results search_paths filter_value path1`
  Adds i18n lookups for the generated results just like `IDB.IQUERY` or `IDB.ILOOKUP`.
* `IDB.SCAN table num_skip max_results path1 path2 path3`
  Outputs all results by skipping over the first `num_skip` entries in the table and then
  outputting up to `max_results`rows.
* `IDB.ISCAN table primary_lang fallback_lang num_skip max_results path1 path2 path3`
  Again, behaves just like `IDB.SCAN` but provides i18n lookup for the given languages.
* `IDB.SHOW_TABLES` reports all tables and their usage statistics.

# Sources
* The library parts can be found in [jupiter-rs](jupiter-rs)
* The example runtime **Jupiter IO** can be found in [jupiter-io](jupiter-io)

## Links
* Documentation: [jupiter](https://docs.rs/jupiter)
* Docker: [Jupiter IO Image](https://hub.docker.com/repository/docker/scireum/jupiter-io)
* Repository: [GitHub](https://github.com/scireum/jupiter)
* Crate: [jupiter-rs](https://crates.io/crates/jupiter)

## Contributions

Contributions as issues or pull requests are always welcome. Please [sign-off](http://developercertificate.org) 
all your commits by adding a line like "Signed-off-by: Name <email>" at the end of each commit, indicating that
you wrote the code and have the right to pass it on as an open source.

## License

**Jupiter** is licensed under the MIT License:

> Permission is hereby granted, free of charge, to any person obtaining a copy
> of this software and associated documentation files (the "Software"), to deal
> in the Software without restriction, including without limitation the rights
> to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
> copies of the Software, and to permit persons to whom the Software is
> furnished to do so, subject to the following conditions:
> 
> The above copyright notice and this permission notice shall be included in
> all copies or substantial portions of the Software.
> 
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
> OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
> THE SOFTWARE.
