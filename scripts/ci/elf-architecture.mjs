#!/usr/bin/env node
// Reads a tar stream (plain or gzip) on standard input - `docker export` of a container, an image layer - and
// checks that every ELF file in it is built for one machine architecture.
//
//   docker export <container> | node scripts/ci/elf-architecture.mjs x86-64
//   docker export <container> | node scripts/ci/elf-architecture.mjs aarch64 app/
//
// Arguments: the expected architecture (x86-64 or aarch64) and, optionally, a path prefix; only files under
// the prefix are checked. Prints the number of ELF files per architecture and every file of another one,
// exits 1 when there is such a file or no ELF file at all. Nothing is unpacked to disk.

import { createGunzip } from 'node:zlib';

const machines = new Map([[0x3e, 'x86-64'], [0xb7, 'aarch64'], [0x03, 'x86'], [0x28, 'arm']]);
const [expected, prefix = ''] = process.argv.slice(2);
if (![...machines.values()].includes(expected)) {
    console.error(`usage: elf-architecture.mjs <${[...machines.values()].join('|')}> [path prefix] < image.tar`);
    process.exit(2);
}

// The stream, with gzip undone when it starts with the gzip magic.
async function* input() {
    const first = await new Promise(resolve => process.stdin.once('readable', () => resolve(process.stdin.read(2))));
    if (first === null) return;
    process.stdin.unshift(first);
    const gzip = first.length === 2 && first[0] === 0x1f && first[1] === 0x8b;
    yield* gzip ? process.stdin.pipe(createGunzip()) : process.stdin;
}

// Hands out exact byte counts from the chunked stream.
const chunks = input();
let buffer = Buffer.alloc(0);
async function take(count) {
    while (buffer.length < count) {
        const { value, done } = await chunks.next();
        if (done) return null;
        buffer = buffer.length === 0 ? value : Buffer.concat([buffer, value]);
    }
    const taken = buffer.subarray(0, count);
    buffer = buffer.subarray(count);
    return taken;
}
async function skip(count) {
    while (count > 0) {
        if (buffer.length === 0) {
            const { value, done } = await chunks.next();
            if (done) return;
            buffer = value;
        }
        const step = Math.min(count, buffer.length);
        buffer = buffer.subarray(step);
        count -= step;
    }
}

const text = bytes => { const end = bytes.indexOf(0); return bytes.subarray(0, end < 0 ? bytes.length : end).toString('utf8'); };
const padded = size => Math.ceil(size / 512) * 512;

const counts = new Map();
const foreign = [];
let longName = null;

for (;;) {
    const header = await take(512);
    if (header === null || header.every(b => b === 0)) break;

    const size = parseInt(text(header.subarray(124, 136)).trim() || '0', 8);
    const type = String.fromCharCode(header[156]);
    let name = text(header.subarray(345, 500));
    name = (name ? name + '/' : '') + text(header.subarray(0, 100));

    // A name longer than the header holds comes in an entry of its own: GNU 'L', or 'path=' in a pax 'x' entry.
    if (type === 'L' || type === 'x') {
        const body = (await take(padded(size))).subarray(0, size);
        if (type === 'L') longName = text(body);
        else longName = /(?:^|\n)\d+ path=([^\n]*)\n/.exec(body.toString('utf8'))?.[1] ?? longName;
        continue;
    }
    if (longName !== null) { name = longName; longName = null; }

    const regular = type === '0' || type === '\0';
    if (!regular || size < 20 || !name.replace(/^\.\//, '').startsWith(prefix)) {
        await skip(padded(size));
        continue;
    }

    const head = await take(20);
    await skip(padded(size) - 20);
    if (head.readUInt32BE(0) !== 0x7f454c46) continue;

    // e_machine, in the byte order the file says it is in (EI_DATA).
    const code = head[5] === 2 ? head.readUInt16BE(18) : head.readUInt16LE(18);
    const machine = machines.get(code) ?? `machine 0x${code.toString(16)}`;
    counts.set(machine, (counts.get(machine) ?? 0) + 1);
    if (machine !== expected) foreign.push(`${name}: ${machine}`);
}

const summary = [...counts].map(([machine, count]) => `${count} ${machine}`).join(', ') || 'none';
console.log(`ELF files${prefix ? ` under ${prefix}` : ''}: ${summary}; expected ${expected}`);
if (foreign.length > 0) {
    console.error(foreign.join('\n'));
    process.exit(1);
}
if (counts.size === 0) {
    console.error('no ELF file found: nothing was checked');
    process.exit(1);
}
