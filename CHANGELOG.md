# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->
## [Unreleased] - ReleaseDate

### Added

- Added the `--stream-encoding` CLI arg to allow choosing between Rkyv and Abomonation encoded network streams (Set to `abomonation` by default for compatibility reasons)
- Added the `--disable-timeline` CLI flag to allow disabling timeline generation, speeding up ddshow and the generated webpage

### Fixed

- Fixed a crash when `TIMELY_WORKER_ADDR` was set within ddshow's environment

<!-- next-url -->
[Unreleased]: https://github.com/Kixiron/lasso/compare/v0.5.1...HEAD
