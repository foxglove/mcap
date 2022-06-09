# Release

How to make releases.

## NPM

- Checkout the version of the code you want to release
- Run this yarn command to tag a new version `yarn workspace @mcap/core version --minor|--major|--patch`
- Make a PR with your changes to package.json
- Wait for the PR to pass CI and merge
- Checkout main and tag the merged commit with `releases/typescript/core/v#.#.#` (replace #.#.# with the version you want to release)
- Push the new tag to the repo with `git push origin releases/typescript/core/v#.#.#`
