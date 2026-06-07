#!/usr/bin/env node
// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0
//
// sync-project.mjs — idempotent sync of `roadmap`-labelled issues into
// the Bowire Project board. Reads every open issue with the
// `roadmap` label, looks each one up in the Project, attaches the
// missing ones, and applies the Status / Area / Track / Priority /
// Kind / Milestone field values from a per-title mapping table.
//
// Safe to re-run after a rate-limit reset, after manual edits in the
// Project UI, and after new roadmap items get filed: anything already
// in the right state is a no-op. Issues whose titles are not in the
// mapping table get attached to the project but receive no field
// values — manual triage from the UI takes over.
//
// Usage:
//   node scripts/sync-project.mjs               # do the sync
//   node scripts/sync-project.mjs --dry-run     # print plan, do nothing
//
// Auth: GH_TOKEN with `project` + `repo` scope, or `gh auth token`.

import { execSync } from "node:child_process";
import process from "node:process";

const ORG = "Kuestenlogik";
const REPO = "Surgewave";
const PROJECT_NUMBER = 4;
const DRY_RUN = process.argv.includes("--dry-run");

const TOKEN = process.env.GH_TOKEN || process.env.GITHUB_TOKEN ||
    (() => { try { return execSync("gh auth token", { stdio: ["ignore", "pipe", "ignore"] }).toString().trim(); } catch { return null; } })();
if (!TOKEN) { console.error("No token. Set GH_TOKEN or run `gh auth login`."); process.exit(1); }

async function gh(query, vars = {}) {
    const res = await fetch("https://api.github.com/graphql", {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${TOKEN}`,
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "User-Agent": "surgewave-project-sync",
        },
        body: JSON.stringify({ query, variables: vars }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
    const json = await res.json();
    if (json.errors) throw new Error(`GraphQL: ${JSON.stringify(json.errors)}`);
    return json.data;
}

// Per-issue-title field mapping + creation body. Issues whose title
// isn't here get attached to the project but no field values (manual
// UI triage takes over). Entries whose title doesn't exist in the repo
// yet are created via REST. milestone is set on creation; labels are
// derived from area/track/kind/priority.
const ROADMAP_URL_BASE = `https://github.com/${ORG}/${REPO}/blob/main/ROADMAP.md`;
function trackedBody(anchor, desc) {
    return `${desc}\n\nTracked in [\`ROADMAP.md\`](${ROADMAP_URL_BASE}#${anchor}) — the narrative there carries the full design rationale + the per-bullet checkboxes. This issue exists for board-level Status / Area / Track / Milestone tracking.`;
}
const MAPPING = {
    "G1 — Native non-.NET clients (Python, Go, Rust)":                       { status: "Backlog", area: "multi",       kind: "feature", priority: "P1" },
    "G3 — Public benchmarks on identical hardware":                          { status: "Next up", area: "broker",      track: "performance",         kind: "feature", priority: "P1" },
    "G4 — Real Jepsen run":                                                  { status: "Backlog", area: "clustering",  track: "cluster-correctness", kind: "feature", priority: "P2" },
    "G12 — Cluster-linking-grade geo-replication":                           { status: "Backlog", area: "clustering",  kind: "feature", priority: "P2" },
    "G15 — CLI polish (remaining)":                                          { status: "Backlog", area: "cli",         kind: "debt",    priority: "P2" },
    "G17 — Flink connector":                                                 { status: "Backlog", area: "connect",     kind: "feature", priority: "P3" },
    "G18 — AI primitives as default-bundled Apache-2.0 plugin":              { status: "Next up", area: "ai",          track: "ai-pipelines",        kind: "feature", priority: "P1" },
    "G21 — Disaggregated compute/storage mode":                              { status: "Backlog", area: "storage",     kind: "feature", priority: "P1" },
    "G23 — Pipeline-as-code (C# DSL)":                                       { status: "Backlog", area: "streams",     kind: "feature", priority: "P2" },
    "G24 — Lineage-driven impact analysis":                                  { status: "Backlog", area: "schema",      kind: "feature", priority: "P2" },
    "G25 — Vector type as first-class schema primitive":                     { status: "Backlog", area: "schema",      track: "ai-pipelines",        kind: "feature", priority: "P1" },
    "G26 — AI pipeline cost tracking":                                       { status: "Backlog", area: "ai",          track: "ai-pipelines",        kind: "feature", priority: "P1" },
    "G28 — Leader-reelection latency after broker shutdown":                 { status: "Next up", area: "clustering",  track: "cluster-correctness", kind: "bug",     priority: "P2" },
    "Plugin SDK B — Schema-validation as build task":                        { status: "Backlog", area: "plugin-sdk",  track: "plugin-distribution", kind: "feature", priority: "P2" },
    "Plugin SDK C — surgewave plugin scaffold + dotnet new templates":       { status: "Backlog", area: "plugin-sdk",  track: "plugin-distribution", kind: "feature", priority: "P2" },
    "Plugin SDK D — surgewave sdk install --version X.Y.Z":                  { status: "Next up", area: "plugin-sdk",  track: "plugin-distribution", kind: "feature", priority: "P1" },
    "Plugin SDK E — Roslyn analysers (SRWV-prefix rules)":                   { status: "Backlog", area: "plugin-sdk",  track: "plugin-distribution", kind: "feature", priority: "P2" },
    "Plugin SDK F — Sample plugin reference repo":                           { status: "Backlog", area: "plugin-sdk",  track: "plugin-distribution", kind: "feature", priority: "P2" },
    "Operator wizard — surgewave setup (interactive CLI)":                   { status: "Backlog", area: "cli",         kind: "feature", priority: "P2" },
    "Operator wizard — Browser variant in Control UI":                       { status: "Backlog", area: "control",     kind: "feature", priority: "P2" },
    "Operator wizard — Plugin marketplace dependency graph":                 { status: "Backlog", area: "plugin-sdk",  kind: "feature", priority: "P2" },
    "QUIC transport benchmark on real LAN/WAN":                              { status: "Backlog", area: "broker",      track: "transport",           kind: "feature", priority: "P2" },
    "QUIC retransmit statistics":                                            { status: "Backlog", area: "observability", track: "transport",         kind: "debt",    priority: "P3" },
    "Branch protection for external PRs":                                    { status: "Backlog", area: "multi",       kind: "debt",    priority: "P2" },
    "Getting-started video (5-minute demo)":                                 { status: "Backlog", area: "docs",        kind: "docs",    priority: "P2" },
    "Control UI license page":                                               { status: "Backlog", area: "control",     kind: "feature", priority: "P2" },
};

// 1. Fetch project metadata (id + fields + options)
const projectQ = `
query($org: String!, $number: Int!) {
  organization(login: $org) {
    projectV2(number: $number) {
      id
      title
      fields(first: 30) {
        nodes {
          ... on ProjectV2SingleSelectField {
            id name
            options { id name }
          }
        }
      }
      items(first: 100) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          content { ... on Issue { id number title } }
        }
      }
    }
  }
}`;

const projectData = (await gh(projectQ, { org: ORG, number: PROJECT_NUMBER })).organization.projectV2;
const PROJECT_ID = projectData.id;

// Build a field-name → { id, options: name→id } lookup.
const FIELDS = {};
for (const f of projectData.fields.nodes) {
    if (!f || !f.options) continue;
    FIELDS[f.name] = { id: f.id, options: Object.fromEntries(f.options.map((o) => [o.name, o.id])) };
}

// items already in the project, keyed by issue node-id
const existingItemByIssueId = new Map();
for (const item of projectData.items.nodes) {
    if (item.content?.id) existingItemByIssueId.set(item.content.id, item.id);
}
// note: project may have >100 items — re-fetch if pagination becomes needed later

// 2. Fetch every open `roadmap` issue in the repo
const issuesQ = `
query($owner: String!, $repo: String!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    issues(first: 100, after: $cursor, states: OPEN, labels: ["roadmap"]) {
      pageInfo { hasNextPage endCursor }
      nodes { id number title }
    }
  }
}`;
const issues = [];
let cursor = null;
while (true) {
    const result = (await gh(issuesQ, { owner: ORG, repo: REPO, cursor })).repository.issues;
    issues.push(...result.nodes);
    if (!result.pageInfo.hasNextPage) break;
    cursor = result.pageInfo.endCursor;
}
console.log(`Found ${issues.length} \`roadmap\`-labelled issues in ${ORG}/${REPO}.`);

// 2b. Create any MAPPING entries that have a `body` set but no matching
// open issue yet. REST POST /repos/owner/repo/issues; labels derived
// from area/track/kind plus the "roadmap" marker; milestone by title.
const issueTitles = new Set(issues.map((i) => i.title));
const milestonesQ = `query($owner: String!, $repo: String!) {
  repository(owner: $owner, name: $repo) { milestones(first: 25, states: OPEN) { nodes { number title } } }
}`;
const milestoneByTitle = Object.fromEntries(
    (await gh(milestonesQ, { owner: ORG, repo: REPO })).repository.milestones.nodes.map((m) => [m.title, m.number])
);

for (const [title, m] of Object.entries(MAPPING)) {
    if (!m.body || issueTitles.has(title)) continue;
    const labels = ["roadmap", `area:${m.area}`, `kind:${m.kind}`];
    if (m.track) labels.push(`track:${m.track}`);
    const payload = { title, body: m.body, labels };
    if (m.milestone && milestoneByTitle[m.milestone]) payload.milestone = milestoneByTitle[m.milestone];

    if (DRY_RUN) { console.log(`[DRY] Would create issue "${title}" with labels ${labels.join(",")}`); continue; }
    const res = await fetch(`https://api.github.com/repos/${ORG}/${REPO}/issues`, {
        method: "POST",
        headers: { Authorization: `Bearer ${TOKEN}`, Accept: "application/vnd.github+json", "Content-Type": "application/json", "User-Agent": "surgewave-project-sync" },
        body: JSON.stringify(payload),
    });
    if (!res.ok) { console.error(`!! Create failed for "${title}": HTTP ${res.status} ${await res.text()}`); continue; }
    const issue = await res.json();
    // GraphQL needs the node_id, not the REST id.
    issues.push({ id: issue.node_id, number: issue.number, title: issue.title });
    console.log(`Created #${issue.number} ${issue.title}`);
}

// 3. For each issue: attach to project if missing, then push field values.
const addMut = `mutation($projectId: ID!, $contentId: ID!) {
  addProjectV2ItemById(input: { projectId: $projectId, contentId: $contentId }) {
    item { id }
  }
}`;
const setMut = `mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $projectId, itemId: $itemId, fieldId: $fieldId,
    value: { singleSelectOptionId: $optionId }
  }) { projectV2Item { id } }
}`;

let attached = 0, fieldsSet = 0, unmapped = 0;
for (const issue of issues) {
    let itemId = existingItemByIssueId.get(issue.id);
    if (!itemId) {
        if (DRY_RUN) { console.log(`[DRY] Would attach #${issue.number} ${issue.title}`); attached++; continue; }
        const r = await gh(addMut, { projectId: PROJECT_ID, contentId: issue.id });
        itemId = r.addProjectV2ItemById.item.id;
        attached++;
        console.log(`Attached #${issue.number} ${issue.title}`);
    }
    const map = MAPPING[issue.title];
    if (!map) { unmapped++; continue; }
    for (const [fieldName, optionName] of [["Status", map.status], ["Area", map.area], ["Track", map.track], ["Priority", map.priority], ["Kind", map.kind]]) {
        if (!optionName) continue;
        const f = FIELDS[fieldName];
        const oid = f?.options?.[optionName];
        if (!oid) { console.warn(`!! Missing option ${fieldName}=${optionName}`); continue; }
        if (DRY_RUN) { fieldsSet++; continue; }
        await gh(setMut, { projectId: PROJECT_ID, itemId, fieldId: f.id, optionId: oid });
        fieldsSet++;
    }
}

console.log(`\nDone. ${attached} attached, ${fieldsSet} field-values set, ${unmapped} issue(s) without a mapping entry.`);
console.log(`Project: https://github.com/orgs/${ORG}/projects/${PROJECT_NUMBER}`);
