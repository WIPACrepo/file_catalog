# Changelog

<!--next-version-placeholder-->

## v1.9.8 (2023-05-30)
### Other

* Bump py-versions CI release v2.1 ([`a118e25`](https://github.com/WIPACrepo/file_catalog/commit/a118e254bc470984b534d66417c43b17b020aed8))

## v1.9.7 (2023-03-28)
### Other
* Allow configuration of service address ([#143](https://github.com/WIPACrepo/file_catalog/issues/143)) ([`52d29a3`](https://github.com/WIPACrepo/file_catalog/commit/52d29a3949015d850963a09b92482e4b8af5c4b4))

## v1.9.6 (2023-03-24)
### Other
* PyMongo 4 is too advanced for us ([#142](https://github.com/WIPACrepo/file_catalog/issues/142)) ([`66d2bb1`](https://github.com/WIPACrepo/file_catalog/commit/66d2bb1e55fd790d8814ecf2e2acb10f3621d096))

## v1.9.5 (2023-03-24)
### Other
* Modify CICD so we can get semver tags on Docker images again ([#141](https://github.com/WIPACrepo/file_catalog/issues/141)) ([`54af1d7`](https://github.com/WIPACrepo/file_catalog/commit/54af1d7a3703056049a746048c26ae6e743e8a8b))

## v1.9.4 (2023-03-22)
### Other
* Development ergonomics ([#140](https://github.com/WIPACrepo/file_catalog/issues/140)) ([`83913f2`](https://github.com/WIPACrepo/file_catalog/commit/83913f259c77903ce5bde421efab9669a4276dd0))

## v1.9.3 (2023-02-28)
### Other
* Keycloak Authentication ([#138](https://github.com/WIPACrepo/file_catalog/issues/138)) ([`6623fb2`](https://github.com/WIPACrepo/file_catalog/commit/6623fb2d5dfb168ce41e002ed0f85bac5d3f63f8))
* <bot> update requirements.txt ([`ce41938`](https://github.com/WIPACrepo/file_catalog/commit/ce41938361a81dfc4ddc74cd073355c40a11be03))
* <bot> update setup.cfg ([`79445ed`](https://github.com/WIPACrepo/file_catalog/commit/79445ed947bd012efc0462f7218a1e328bd8cc46))
* Add CodeQL workflow for GitHub code scanning ([#136](https://github.com/WIPACrepo/file_catalog/issues/136)) ([`e0cde7e`](https://github.com/WIPACrepo/file_catalog/commit/e0cde7eb3b7dfeb3e34b71e0bc990a799979ebcf))
* Bump certifi from 2021.10.8 to 2022.12.7 ([#137](https://github.com/WIPACrepo/file_catalog/issues/137)) ([`83ef4d2`](https://github.com/WIPACrepo/file_catalog/commit/83ef4d2f35a45d87ed481595a9c0394453adbf84))

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

## [1.2.1] - 2020-02-11
### Added
- Instructions on building File Catalog as a docker container
- Instructions on deploying a File Catalog container to WIPAC Kubernetes
- Test for MongoDB unindexed queries
- MongoDB hashed index for logical_name field
### Changed
- Many improvements to the Metadata Import script

## [1.2.0] - 2020-02-03
### Added
- Use MONGODB_AUTH_SOURCE_DB to specify the DB with authentication details
### Changed
- Additional changes not specified; use the git log, Luke

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

[Unreleased]: https://github.com/WIPACrepo/file_catalog/compare/1.2.1...HEAD
[1.2.1]: https://github.com/WIPACrepo/file_catalog/compare/1.2.0...1.2.1
[1.2.0]: https://github.com/WIPACrepo/file_catalog/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/WIPACrepo/file_catalog/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/WIPACrepo/file_catalog/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/WIPACrepo/file_catalog/compare/0.1.0...1.0.0
