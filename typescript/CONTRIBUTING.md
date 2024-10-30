# Development guide

Install dependencies:

```
corepack enable
yarn install
```

Run lint/tests:

```
yarn workspace @mcap/core lint
yarn workspace @mcap/core test
```

Read and validate an MCAP file:

```
yarn workspace @foxglove/mcap-example-validate validate file.mcap
```

Run benchmarks:

```
yarn workspace @foxglove/mcap-benchmarks bench
```

Run benchmarks with Chrome debugger attached to use profiling tools:

```
yarn workspace @foxglove/mcap-benchmarks bench:debug
```

## Releasing to NPM

- Check out the version of the code you want to release
- Update package.json in `typescript/{pkg}/package.json` with the new version.
- Make a PR with your changes to package.json
- Wait for the PR to pass CI and merge
- Checkout main and tag the merged commit with `releases/typescript/{pkg}/v#.#.#` (replace #.#.# with the version you used in package.json)
- Push the new tag to the repo with `git push origin releases/typescript/{pkg}/v#.#.#`
