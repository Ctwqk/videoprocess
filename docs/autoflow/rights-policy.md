# AutoFlow Rights Policy

AutoFlow is safe by default. It may plan previews automatically, but publication
is gated by source policy, candidate rights, publish mode, and human review.

## Defaults

`AutoFlowRequest` defaults to:

- `source_policy=owned_only`
- `publish_mode=preview_only`
- `aspect_ratio=auto`, resolved by the parser to a concrete output ratio

The planner should prefer owned assets, licensed assets, or user-uploaded
materials. External platform material is for research or review workflows only
unless a later policy explicitly allows it.

## Source Policies

| Policy | Meaning | Expected candidate behavior |
| --- | --- | --- |
| `owned_only` | Use owned or library-backed assets only. | External URL candidates are blocked. |
| `licensed_only` | Use assets with known license grants. | Candidate selector must preserve license metadata. |
| `public_domain_or_cc` | Use assets with explicit public-domain or Creative Commons rights. | Candidate selector must preserve attribution/license metadata. |
| `research_only` | External search/download material can be used for draft research. | Requires review before publication. |
| `remix_with_review` | Remix from external or mixed sources, but require review. | Requires review before execution/publication depending on publish mode. |

## Publish Modes

| Mode | Policy |
| --- | --- |
| `preview_only` | Lowest-risk default. No upload node is required. |
| `private_upload` | Allowed for owned/library-backed material; external sources still require review. |
| `unlisted_upload` | Allowed for owned/library-backed material; external sources still require review. |
| `public_after_review` | Always requires explicit human approval. Upload privacy must remain conservative until approval. |

## Current Decision Matrix

| Condition | Status | Execution | Publication |
| --- | --- | --- | --- |
| `owned_only` request includes external URL candidates | `blocked` | Not allowed | Not allowed |
| Unknown rights with `public_after_review` | `review_required` | Allowed | Not allowed until review |
| Any external URL or external platform candidate | `review_required` | Allowed for draft/review flows | Only preview/private/unlisted after review policy allows it |
| Owned/library-backed candidates with preview/private/unlisted mode | `allowed` | Allowed | Allowed only for requested safe mode |
| Any request for `public_after_review` | `review_required` | Allowed | Not allowed until human approval |

## Human Review Expectations

Review should confirm:

- Source ownership or license.
- Whether external clips can be reused.
- Required attribution and platform restrictions.
- Metadata, title, tags, and thumbnail text.
- Final publish target and privacy mode.

The service represents this gate as `plan.needs_review`. Approval updates the
stored plan, but approval does not override a `blocked` rights decision.

## Engineering Constraints

- Do not add public upload behavior as a default.
- Do not infer broad reuse rights from a URL alone.
- Do not let candidate selectors silently downgrade rights status.
- Keep external platform assets out of public publication unless explicit human
  review and policy allow it.
- When adding new publish nodes, default privacy to `private` or `unlisted`.
