# Release

How to make releases.

## @mcap/core

- Checkout the version of the code you want to release
- Update package.json in typescript/core/package.json with the new version.
- Update package.json in typescript/support/package.json to reference the new version of @mcap/core
- Make a PR with your changes to both package.json files
- Wait for the PR to pass CI and merge
- Checkout main and tag the merged commit with `releases/typescript/core/v#.#.#` (replace #.#.# with the version you used in package.json)
- Tag the merged commit with `releases/typescript/support/v#.#.#` (replace #.#.# with the version you used in support/package.json)
- Push the new tags to the repo with `git push origin releases/typescript/core/v#.#.#` and `git push origin releases/typescript/support/v#.#.#`

## @mcap/support

- Checkout the version of the code you want to release
- Update package.json in typescript/support/package.json with the new version.
- Make a PR with your changes to package.json
- Wait for the PR to pass CI and merge
- Checkout main and tag the merged commit with `releases/typescript/support/v#.#.#` (replace #.#.# with the version you used in package.json)
- Push the new tag to the repo with `git push origin releases/typescript/support/v#.#.#`
