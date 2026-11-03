# Security Policy

## Supported Versions

Security fixes are applied to the latest minor release of each actively
maintained project. Older releases and archived repositories do not receive
security updates; please upgrade before reporting issues against unmaintained
versions.

| Project | Supported |
| --- | --- |
| [foxglove/mcap](https://github.com/foxglove/mcap) (MCAP libraries and CLI, all language implementations in this repo) | Latest minor release only |
| [foxglove/foxglove-sdk](https://github.com/foxglove/foxglove-sdk) | Latest minor release only |
| Other actively maintained `foxglove/*` open source repositories | Latest release; best effort |
| Archived repositories (`ws-protocol`, `rosbag`, `rosmsg`, `xmlrpc`, `nuscenes2bag`, and any repo GitHub marks as archived) | :x: Not supported |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Foxglove does not currently publish a dedicated security contact address
(e.g. a `security@` mailbox) on [foxglove.dev](https://foxglove.dev), so the
preferred disclosure channel for the open source projects above is
[GitHub private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability):
open the repository's **Security** tab and choose **Report a vulnerability**.
This delivers the report privately to the maintainers with a coordinated
disclosure workflow built in.

If private vulnerability reporting is unavailable on a given repository, open a
minimal public issue asking the maintainers to enable it — **do not include
details of the vulnerability** in the public issue.

### Scope notes

- Reports should target the open source code in the repository you are
  reading. The Foxglove Studio desktop application and the Foxglove cloud
  service are closed-source products: vulnerabilities in them should be
  reported to [Foxglove](https://foxglove.dev/contact) directly, not to the
  open source repositories, which cannot patch them.
- When reporting, include the affected repository and version, a description
  of the impact, and, if possible, a proof of concept or reproduction steps.

## Response Timeline

We aim to:

- Acknowledge receipt of a report within **5 business days**.
- Provide an initial assessment (confirmed / not reproducible / needs more
  information) within **2 weeks**.
- Coordinate a fix and disclosure timeline with the reporter before any
  public announcement. We ask reporters to maintain confidentiality until a
  fix is released and reasonable time has been given for users to upgrade.

## Safe Harbor

Good-faith security research that follows this policy will be treated
appropriately: we will not pursue legal action against, or report to law
enforcement, researchers who make a good faith effort to comply with this
policy.
