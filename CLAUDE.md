# Releases

The SDK releases through Shipyard, as a Go library. Nobody types a version, cuts a branch or makes a tag by hand.

1. **Every PR into `develop` carries a `Changelog:` line** in its body (the PR template has it). It is one or two accurate sentences a plugin author can read. A PR that changes only files the release does not ship (`notShipped` in `.github/shipyard.yml`, such as tests, `docs/` and `.github/`) may leave the line out.
2. **Never edit `CHANGELOG.md` in a PR.** The release writes its section from the `Changelog:` lines of the PRs it ships.
3. **The release starts on its own** once a week when a shipped change is waiting: Shipyard files a prep job, and the prep runs through `.github/workflows/shipyard-release.yml`, which cuts a release PR into `main`. Its record runs the SDK's tests and builds `turbot/tailpipe` against the candidate, and says SHIP or HOLD.
4. **The approval ask posts in #tailpipe.** Merging the approved release PR makes the tag, which is the publish: the Go proxy serves the version once the tag exists.
5. **Do not edit `.github/workflows/shipyard-pr.yml` or `.github/workflows/shipyard-release.yml`.** They are the same in every repo.
