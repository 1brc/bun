import * as os from 'node:os';
import * as fsp from 'node:fs/promises';
import * as util from 'node:util';
import * as fs from 'node:fs';
import * as workerThreads from 'node:worker_threads';

const fsRead: (
  fd: number,
  buffer: Uint8Array,
  offset: number,
  length: number,
  position: number
) => Promise<number> = util.promisify(fs.read);

const fsClose: (fd: number) => Promise<void> = util.promisify(fs.close);

type CalcResultsCont = Map<
  string,
  { min: number; max: number; sum: number; count: number }
>;

const MAX_LINE_LENGTH = 100 + 1 + 4 + 1;

/** @type {(...args: any[]) => void} */
const debug = process.env.DEBUG ? console.error : () => {};

const fileName = process.argv[2];

const fd = await fsp.open(fileName);
const size = (await fsp.stat(fileName)).size;

// const threadsCount = os.cpus().length;
const threadsCount = 1;

const chunkSize = Math.floor(size / threadsCount);

let chunkOffsets: number[] = [];

let offset = 0;
const bufFindNl = Buffer.alloc(MAX_LINE_LENGTH);

while (true) {
  offset += chunkSize;

  if (offset >= size) {
    chunkOffsets.push(size);
    break;
  }

  await fsRead(fd, bufFindNl, 0, MAX_LINE_LENGTH, offset);

  const nlPos = bufFindNl.indexOf(10);
  bufFindNl.fill(0);

  if (nlPos === -1) {
    chunkOffsets.push(size);
    break;
  } else {
    offset += nlPos + 1;
    chunkOffsets.push(offset);
  }
}

await fsClose(fd);

const compiledResults: CalcResultsCont = new Map();

let completedWorkers = 0;

for (let i = 0; i < chunkOffsets.length; i++) {
  const worker = new Worker(import.meta.resolveSync('./worker.js'), {});

  worker.postMessage({
    fileName,
    start: i === 0 ? 0 : chunkOffsets[i - 1],
    end: chunkOffsets[i],
  });

  worker.addEventListener('message', (event): void => {
    const message = event.data as CalcResultsCont;

    console.log(`Got map from worker: ${message.size}`);

    worker.unref();

    for (let [key, value] of message.entries()) {
      const existing = compiledResults.get(key);
      if (existing) {
        existing.min = Math.min(existing.min, value.min);
        existing.max = Math.max(existing.max, value.max);
        existing.sum += value.sum;
        existing.count += value.count;
      } else {
        compiledResults.set(key, value);
      }
    }

    completedWorkers++;
    if (completedWorkers === chunkOffsets.length) {
      printCompiledResults(compiledResults);
    }
  });

  worker.addEventListener('messageerror', (event) => {
    console.error(event);
  });

  worker.addEventListener('open', (event) => {
    debug('Worker started');
  });

  worker.addEventListener('error', (err) => {
    console.error(err);
  });

  worker.addEventListener('close', (event) => {
    debug('Worker stopped');
  });
}

/**
 * @param {CalcResultsCont} compiledResults
 */
function printCompiledResults(compiledResults: CalcResultsCont) {
  const sortedStations = Array.from(compiledResults.keys()).sort();

  process.stdout.write('{');
  for (let i = 0; i < sortedStations.length; i++) {
    if (i > 0) {
      process.stdout.write(', ');
    }
    const data = compiledResults.get(sortedStations[i])!;
    process.stdout.write(sortedStations[i]);
    process.stdout.write('=');
    process.stdout.write(
      round(data.min / 10) +
        '/' +
        round(data.sum / 10 / data.count) +
        '/' +
        round(data.max / 10)
    );
  }
  process.stdout.write('}\n');
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 * @returns {string}
 */
function round(num: number) {
  const fixed = Math.round(10 * num) / 10;

  return fixed.toFixed(1);
}
