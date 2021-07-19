# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

### Added

- Added the `--stream-encoding` CLI arg to allow choosing between Rkyv and Abomonation encoded
  network streams (Set to `abomonation` by default for compatibility reasons)
- Added the `--disable-timeline` CLI flag to allow disabling timeline generation, speeding up
  ddshow and the generated webpage
- Allowed `--replay-logs` to be given multiple times, allowing multiple target folders to be given
- Added vega-based dashboard
- Added the `--completions` arg to allow generating shell completions for ddshow
- Added the `--quiet` CLI flag to disable ddshow's terminal output
- Added tty detection, `--quiet` is on for ttys (stdout isn't printed to) and `--color=auto`
  will turn into `--color=never` (ansi escapes aren't emitted unless `--color=always` is passed)

### Changed

- [Breaking] Changed termination method from reading stdin to a ctrl+c/`SIGINT`/`CTRL_C_EVENT`/`CTRL_BREAK_EVENT` handler

### Fixed

- Fixed a crash when `TIMELY_WORKER_ADDR` was set within ddshow's environment
- Fixed operator activation durations (aggregated and per-worker) being deduplicated

<!-- next-url -->
[Unreleased]: https://github.com/Kixiron/lasso/compare/v0.5.1...HEAD
