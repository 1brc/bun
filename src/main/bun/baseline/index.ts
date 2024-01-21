const fileName = Bun.argv[2];

type AggregationsMap = Map<
  string,
  { min: number; max: number; sum: number; count: number }
>;
const aggregations: AggregationsMap = new Map();

readLineByLine(
  Bun.file(fileName).stream(),
  (line) => {
    const [stationName, temperatureStr] = line.split(';');

    // use integers for computation to avoid loosing precision
    const temperature = Math.floor(parseFloat(temperatureStr) * 10);

    const existing = aggregations.get(stationName);

    if (existing) {
      existing.min = Math.min(existing.min, temperature);
      existing.max = Math.max(existing.max, temperature);
      existing.sum += temperature;
      existing.count++;
    } else {
      aggregations.set(stationName, {
        min: temperature,
        max: temperature,
        sum: temperature,
        count: 1,
      });
    }
  },
  () => {
    printCompiledResults(aggregations);
  }
);

function printCompiledResults(aggregations: AggregationsMap) {
  const sortedStations = Array.from(aggregations.keys()).sort();

  let result =
    '{' +
    sortedStations
      .map((station) => {
        const data = aggregations.get(station)!;

        return `${station}=${round(data.min / 10)}/${round(
          data.sum / 10 / data.count
        )}/${round(data.max / 10)}`;
      })
      .join(', ') +
    '}';

  console.log(result);
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 *
 * @returns {string}
 */
function round(num: number): string {
  const fixed = Math.round(10 * num) / 10;

  return fixed.toFixed(1);
}

async function readLineByLine(
  stream: ReadableStream<Uint8Array>,
  cb: (line: string) => void,
  done: () => void
) {
  const nlChar = '\n'.charCodeAt(0);
  const textDecoder = new TextDecoder();

  // max line length is 128
  let lineBuffer = new Uint8Array(128);
  let lineBufferLen = 0;

  for await (const chunk of stream) {
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] === nlChar) {
        const line = textDecoder.decode(lineBuffer.slice(0, lineBufferLen));
        lineBufferLen = 0;
        cb(line);
      } else {
        lineBuffer[lineBufferLen] = chunk[i];
        lineBufferLen++;
      }
    }
  }

  // dont emit an empty string for files that end with a new line
  if (lineBufferLen > 0) {
    cb(textDecoder.decode(lineBuffer.slice(0, lineBufferLen)));
  }

  done();
  return;
}
