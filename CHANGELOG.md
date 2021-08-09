# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

## [0.2.2] - 2021-08-09

## [0.2.1] - 2021-08-09

## [0.2.0] - 2021-08-09

### Added

- Added the `--stream-encoding` CLI arg to allow choosing between Rkyv and Abomonation encoded network streams
  (Set to `abomonation` by default for compatibility reasons)
- Added the `--disable-timeline` CLI flag to allow disabling timeline generation, speeding up ddshow and
  the generated webpage
- Allowed `--replay-logs` to be given multiple times, allowing multiple target folders to be given
- Added vega-based dashboard
- Added the `--completions` arg to allow generating shell completions for ddshow
- Added the `--quiet` flag that disables terminal output

### Changed

- DDShow no longer listens for an enter on the command line, DDShow now listens for the ctrl+c signal
  to terminate computation
- DDShow automatically enables `--quiet` when not pointed at a tty

### Fixed

- Fixed a crash when `TIMELY_WORKER_ADDR` was set within ddshow's environment
- Fixed abysmal performance on large dataflow graphs

<!-- next-url -->
[Unreleased]: https://github.com/Kixiron/ddshow/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/Kixiron/ddshow/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Kixiron/ddshow/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Kixiron/ddshow/releases/tag/v0.1.1...v0.2.0
