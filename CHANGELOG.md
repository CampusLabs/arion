# Change Log
All notable changes to this project will be documented in this file. This
change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

## [1.3.0] - 2016-09-09
### Added
- Websocket message production
- Maximum GC pause environment variable
- Durable queue fsync and slab size environment variables

### Changed
- Greatly reduced the number of threads created under high connection
  count loads (thereby reducing the number of 503 errors returned to the
  user) by queueing all durable queue writes. Since writes block, this
  means that only one thread is blocked waiting on disk to enqueue
  rather than potentially hundreds.
- Improve topic and key validation responses

### Fixed
- Include executor queue request errors in server error metrics
- Kafka producer now blocks when its queue is full rather than raising
  an exception

## [1.2.0] - 2016-05-05
### Added
- Maximum message size option
- Batched metrics

### Fixed
- Don't retry messages with fatal errors

## [1.1.0] - 2016-02-21
### Added
- Connection idle timeout, enabled by default
- Better metrics
- Minor performance improvement in API

### Changed
- Open sourced under Apache 2.0 license
- Switched to G1 garbage collector in docker container
- Upgraded to Kafka 0.9.0.1 client
- Added `HEAP_SIZE` environment variable for docker container

## 1.0.0 - 2016-02-01
### Initial Release

[Unreleased]: https://github.com/orgsync/arion/compare/1.3.0...HEAD
[1.3.0]: https://github.com/orgsync/arion/compare/1.2.0...1.3.0
[1.2.0]: https://github.com/orgsync/arion/compare/1.1.0...1.2.0
[1.1.0]: https://github.com/orgsync/arion/compare/1.0.0...1.1.0
