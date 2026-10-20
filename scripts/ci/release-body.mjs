#!/usr/bin/env node
// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0
//
// The body a release is published with: the curated notes, headed by the title of that delivery.
// (Ported from Bowire's scripts/ci/release-body.mjs.)
//
// The release used to be named `<tag> — <milestone theme>`. A milestone is a work section that
// several deliveries come out of, so the theme did not name the delivery, and GitHub truncates the
// release name anyway. The notes file names the delivery in its front-matter `title:`; that title
// is the body's one `#` heading, the `##` sections of the notes sit under it, and the release name
// is the version alone — the one thing the Releases list has room for.
//
// Usage: node scripts/ci/release-body.mjs <notes-file>          the body, headed by its title
//        node scripts/ci/release-body.mjs --title <notes-file>  the title alone, or nothing
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/**
 * The body without its YAML front-matter, and without the blank line the block leaves behind.
 * Markdown has no use for leading space either way, and the publisher would otherwise open the
 * release with it.
 */
export function stripFrontMatter(text) {
  const s = String(text ?? '');
  if (!s.startsWith('---')) return s.replace(/^\s+/, '');
  const end = s.indexOf('\n---', 3);
  // An unterminated block is a broken file. Swallowing the rest of it would publish an empty
  // release and read as notes nobody wrote, so it comes back untouched and visibly wrong.
  if (end < 0) return s;
  return s.slice(s.indexOf('\n', end + 1) + 1).replace(/^\s+/, '');
}

/**
 * A title that says nothing a reader could not read off the tag: the template placeholder, the
 * bare version, "Surgewave v0.6.0". Published as a heading these are noise, so this is a state to
 * catch, not to render.
 */
export function isPlaceholderTitle(title) {
  const t = String(title ?? '').trim();
  if (!t) return true;
  if (t.startsWith('<')) return true;                       // <fill in before the tag>
  return /^(surgewave\s+)?v?\d+\.\d+(\.\d+)?$/i.test(t);    // 0.6.0 / v0.6.0 / Surgewave v0.6.0
}

/** The `title:` from the notes' front-matter, or null when there is none worth printing. */
export function frontMatterTitle(text) {
  const s = String(text ?? '');
  if (!s.startsWith('---')) return null;
  const end = s.indexOf('\n---', 3);
  if (end < 0) return null;
  const line = s.slice(0, end).split('\n').find(l => /^title:\s*/.test(l));
  if (!line) return null;
  const value = line.replace(/^title:\s*/, '').trim().replace(/^["']|["']$/g, '');
  return isPlaceholderTitle(value) ? null : value;
}

/**
 * The published body: the delivery's title as the one `#`, then the curated markdown. A body that
 * already opens with its own `#` is left alone — the file is then the author's to arrange, and a
 * second heading above the first would be two titles.
 */
export function compose(text) {
  const body = stripFrontMatter(text);
  const title = frontMatterTitle(text);
  if (!title) return body;
  if (/^#\s/.test(body)) return body;
  return `# ${title}\n\n${body}`;
}

/** The release name: the version alone, because that is what the Releases list can show. */
export const releaseName = tag => String(tag ?? '').replace(/^v/, '');

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const args = process.argv.slice(2);
  const titleOnly = args[0] === '--title';
  const file = titleOnly ? args[1] : args[0];
  if (!file) {
    console.error('usage: release-body.mjs [--title] <notes-file>');
    process.exit(2);
  }
  const text = readFileSync(file, 'utf8');
  process.stdout.write(titleOnly ? (frontMatterTitle(text) ?? '') : compose(text));
}
