# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- for new features
### Changed
- for changes in existing functionality
### Deprecated
- for soon-to-be removed features
### Removed
- for now removed features
### Fixed
- for any bug fixes
### Security
- in case of vulnerabilities

## [1.1.1] - 2019-08-13
### Added
- All configuration is now loaded from environment variables
### Changed
- Configuration parameter names changed to be more descriptive
### Removed
- Configuration via command-line and config files is no longer possible

## [1.1.0] - 2018-08-07
### Added
- Configuration for flake8 linting tool in setup.cfg
- MongoDB index on 'locations'; a unique multikey index
- Unit tests to ensure uniqueness constraint is enforced properly
### Changed
- MongoDB index on 'logical_name'; the index is now unique
- logical_name is now required to be unique for POST /api/files
- logical_name is now required to be unique for PUT /api/files/{uuid}
- logical_name is now required to be unique for PATCH /api/files/{uuid}
- locations are now required to be unique for POST /api/files
- locations are now required to be unique for PUT /api/files/{uuid}
- locations are now required to be unique for PATCH /api/files/{uuid}
### Fixed
- Issue #33: New file restrictions

## [1.0.0] - 2018-08-03
### Added
- TODO: Add these changes to the ChangeLog
### Changed
- TODO: Add these changes to the ChangeLog
### Deprecated
- TODO: Add these changes to the ChangeLog
### Removed
- TODO: Add these changes to the ChangeLog
### Fixed
- TODO: Add these changes to the ChangeLog
### Security
- TODO: Add these changes to the ChangeLog

## 0.1.0 - 2018-01-08
### Fixed
- Issue #26: 'location.archive' overwriting search query

[Unreleased]: https://github.com/WIPACrepo/file_catalog/compare/1.1.0...HEAD
[1.1.0]: https://github.com/WIPACrepo/file_catalog/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/WIPACrepo/file_catalog/compare/0.1.0...1.0.0
