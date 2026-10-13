#!/usr/bin/env node
// Language guard: everything in this repository is English — code, comments,
// identifiers, config comments, docs, tests, CI files. Another language is only
// allowed as a translation resource or as payload/domain data, and then only
// through an entry in the allowlist that says why.
//
// Usage: node scripts/check-english.mjs [--all] [--loose]
//   Scans every tracked text file and fails with file:line for each line that
//   reads as German. --all also prints lines that the allowlist excuses.
//   --loose lowers the bar (any two German words, or one decisive word) to
//   review a translation for short leftovers; expect false positives there.
//
// Allowlist: .github/language-allowlist.txt, one entry per line:
//   <path glob> | <line regex or *> | <reason>
// The fields are separated by " | " (a bar with a blank on each side). The
// reason is mandatory; an entry without one is an error. Blank lines and
// lines starting with # are ignored.

import { execFileSync } from 'node:child_process';
import { readFileSync, existsSync } from 'node:fs';
import { join, extname, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = execFileSync('git', ['rev-parse', '--show-toplevel'], { encoding: 'utf8' }).trim();
const showAll = process.argv.includes('--all');
const loose = process.argv.includes('--loose');
const allowlistPath = join(root, '.github', 'language-allowlist.txt');

const binaryExt = new Set([
  '.png', '.jpg', '.jpeg', '.gif', '.ico', '.svg', '.webp', '.bmp', '.woff', '.woff2', '.ttf', '.otf', '.eot',
  '.dll', '.exe', '.so', '.dylib', '.zip', '.gz', '.tgz', '.7z', '.pdf', '.swpkg', '.nupkg', '.snupkg', '.snk',
  '.pfx', '.p12', '.bin', '.dat', '.mp4', '.webm', '.mov', '.parquet', '.arrow', '.db', '.wasm', '.onnx', '.avro',
]);

// Words that are function words in German and (nearly) never appear in English prose or code.
const german = new Set((
  'und nicht oder wird werden wurde wurden sich auch eine einen einem einer eines dass daß kein keine keinen ' +
  'keiner wenn weil damit ohne noch schon aber sind kann können koennen muss müssen muessen soll sollte sollen ' +
  'beim vom zum zur für fuer über ueber ist auf bei aus dem den der das nur hier jetzt dann diese dieser dieses ' +
  'diesen nach mehr gibt geht zwischen gegen durch wieder bereits immer liegt steht zurück zurueck jeder jede ' +
  'jedes wir mit ein im zu von bis sowie etwa erst dort hat haben sein seine ihre ihr wo wie als'
).split(' '));
// The subset that settles it on its own when a second German word is on the line.
const decisive = new Set((
  'und nicht oder wird werden wurde wurden sich auch dass daß wenn weil damit ohne noch schon aber sind kann ' +
  'können koennen muss müssen muessen soll sollte sollen beim zum zur für fuer über ueber ist diese dieser ' +
  'dieses diesen wir nur keine keinen jetzt bereits'
).split(' '));
// Umlaut words that are names, not German text.
const names = /K[uü]e?stenlogik|M[uü]nchen|Z[uü]rich|K[oö]ln|D[uü]sseldorf|N[uü]rnberg|G[oö]teborg|Malm[oö]|J[uü]rgen|Bj[oö]rn|G[oö]del|Schr[oö]dinger|Br[uü]ck|Stra[sß]e/gi;
const umlautWord = /[A-Za-zÄÖÜäöüß]*[äöüÄÖÜß][A-Za-zÄÖÜäöüß]*/;
const translit = /\b\w*(fuer|ueber|koenn|muess|waehr|groess|aender|pruef|loesch|oeffn|zurueck|naechst|moegl|ausfuehr|hinzufueg|eintraeg|schluess|gueltig|verfueg|aufloes|zaehl|laeng|gehoer|frueh|spaet|aehnl)\w*\b/i;
const wordRe = /[A-Za-zÄÖÜäöüß]+/g;
// German abbreviations (z. B., u. a., bzw., ggf., usw., d. h., vgl., inkl.) settle it on their own.
const abbrev = /(^|[\s(])(z\.\s?B\.|u\.\s?a\.|bzw\.|ggf\.|usw\.|d\.\s?h\.|vgl\.|inkl\.)/;

export function isGerman(line) {
  const stripped = line.replace(names, '');
  const words = (stripped.match(wordRe) ?? []).map(w => w.toLowerCase());
  const hits = new Set(words.filter(w => german.has(w)));
  const decisiveHits = [...hits].filter(w => decisive.has(w)).length;
  if (hits.size >= 3) return true;
  if (loose && (hits.size >= 2 || decisiveHits >= 1)) return true;
  if (hits.size >= 2 && decisiveHits >= 1) return true;
  if (umlautWord.test(stripped)) return true;
  if (abbrev.test(stripped)) return true;
  if (translit.test(stripped) && hits.size >= 1) return true;
  return false;
}

function globToRegex(glob) {
  let re = '';
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i];
    if (c === '*') {
      if (glob[i + 1] === '*') { re += '.*'; i++; if (glob[i + 1] === '/') i++; }
      else re += '[^/]*';
    } else if (c === '?') re += '[^/]';
    else re += c.replace(/[.+^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`^${re}$`);
}

function loadAllowlist() {
  if (!existsSync(allowlistPath)) return [];
  const entries = [];
  readFileSync(allowlistPath, 'utf8').split(/\r?\n/).forEach((raw, i) => {
    const line = raw.trim();
    if (!line || line.startsWith('#')) return;
    // Fields are separated by " | " (blanks around the bar), so a regex may use | and \| freely.
    const fields = line.split(' | ');
    const parts = fields.length > 3 ? [fields[0], fields[1], fields.slice(2).join(' | ')] : fields;
    parts.forEach((p, k) => { parts[k] = p.trim(); });
    if (parts.length < 3 || !parts[2]) {
      console.error(`.github/language-allowlist.txt:${i + 1}: an entry needs "<path glob> | <line regex or *> | <reason>"`);
      process.exitCode = 1;
      return;
    }
    entries.push({ path: globToRegex(parts[0]), line: parts[1] === '*' ? null : new RegExp(parts[1]) });
  });
  return entries;
}

const self = relative(root, fileURLToPath(import.meta.url)).split('\\').join('/');
const allow = loadAllowlist();
const files = execFileSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8', maxBuffer: 1 << 28 })
  .split('\0').filter(Boolean);

let findings = 0;
for (const file of files) {
  if (binaryExt.has(extname(file).toLowerCase())) continue;
  if (file.endsWith('package-lock.json') || /\.min\.(js|css)$/.test(file)) continue;
  // The guard's own word lists and the allowlist are German by design.
  if (file === self || file === '.github/language-allowlist.txt') continue;
  const full = join(root, file);
  if (!existsSync(full)) continue;
  const buf = readFileSync(full);
  if (buf.subarray(0, 8000).includes(0)) continue;
  const lines = buf.toString('utf8').split(/\r?\n/);
  const fileAllow = allow.filter(a => a.path.test(file));
  lines.forEach((text, i) => {
    if (text.length > 4000 || !isGerman(text)) return;
    const excused = fileAllow.some(a => a.line === null || a.line.test(text));
    if (excused && !showAll) return;
    if (!excused) findings++;
    console.log(`${file}:${i + 1}:${excused ? ' (allowed)' : ''} ${text.trim().slice(0, 200)}`);
  });
}

if (findings > 0) {
  console.error(`\n${findings} line(s) read as German. Everything in this repository is English; ` +
    'another language only as a translation resource or payload data, listed in ' +
    '.github/language-allowlist.txt with a reason.');
  process.exitCode = 1;
} else if (!process.exitCode) {
  console.log('Language guard: no German outside the allowlist.');
}
