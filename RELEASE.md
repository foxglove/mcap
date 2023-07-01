# Release

How to make releases.

## NPM

- Checkout the version of the code you want to release
- Update package.json in typescript/{pkg}/package.json with the new version.
- Make a PR with your changes to package.json
- Wait for the PR to pass CI and merge
- Checkout main and tag the merged commit with `releases/typescript/{pkg}/v#.#.#` (replace #.#.# with the version you used in package.json)
- Push the new tag to the repo with `git push origin releases/typescript/{pkg}/v#.#.#`
