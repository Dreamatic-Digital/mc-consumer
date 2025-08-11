/// <reference types="@cloudflare/workers-types" />
export interface Env {
  MAILCHIMP_API_KEY: string;
  MAILCHIMP_LIST_ID: string;
  DLQ: Queue; // dead-letter queue producers
}

const GROUP_SIZE = 10;
const INTER_GROUP_PAUSE_MS = 250;
const MAX_ATTEMPTS_BEFORE_DLQ = 5;
const BASE_BACKOFF_SECONDS = 30; // tune as needed; used for retry backoff

function md5LowerHex(input: string): string {
  // Minimal MD5 implementation (RFC 1321) for Workers (no external deps)
  function toUtf8Bytes(str: string): Uint8Array {
    return new TextEncoder().encode(str);
  }
  function rotl(x: number, n: number) { return (x << n) | (x >>> (32 - n)); }
  function toHex(num: number) {
    const s = (num >>> 0).toString(16).padStart(8, '0');
    return s.slice(6,8) + s.slice(4,6) + s.slice(2,4) + s.slice(0,2);
  }
  function cmn(q: number, a: number, b: number, x: number, s: number, t: number) {
    return (rotl((a + q + x + t) | 0, s) + b) | 0;
  }
  function ff(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
    return cmn((b & c) | (~b & d), a, b, x, s, t);
  }
  function gg(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
    return cmn((b & d) | (c & ~d), a, b, x, s, t);
  }
  function hh(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
    return cmn(b ^ c ^ d, a, b, x, s, t);
  }
  function ii(a: number, b: number, c: number, d: number, x: number, s: number, t: number) {
    return cmn(c ^ (b | ~d), a, b, x, s, t);
  }

  const msg = toUtf8Bytes(input.toLowerCase());
  const len = msg.length;
  const withOne = new Uint8Array(((len + 9 + 63) >> 6) << 6); // pad to 64-byte block
  withOne.set(msg);
  withOne[len] = 0x80;
  const bitLen = len * 8;
  // little-endian length
  new DataView(withOne.buffer).setUint32(withOne.length - 8, bitLen >>> 0, true);
  new DataView(withOne.buffer).setUint32(withOne.length - 4, Math.floor(bitLen / 0x100000000) >>> 0, true);

  let a = 0x67452301, b = 0xefcdab89, c = 0x98badcfe, d = 0x10325476;

  const K = new Int32Array([
    0xd76aa478,0xe8c7b756,0x242070db,0xc1bdceee,0xf57c0faf,0x4787c62a,0xa8304613,0xfd469501,
    0x698098d8,0x8b44f7af,0xffff5bb1,0x895cd7be,0x6b901122,0xfd987193,0xa679438e,0x49b40821,
    0xf61e2562,0xc040b340,0x265e5a51,0xe9b6c7aa,0xd62f105d,0x02441453,0xd8a1e681,0xe7d3fbc8,
    0x21e1cde6,0xc33707d6,0xf4d50d87,0x455a14ed,0xa9e3e905,0xfcefa3f8,0x676f02d9,0x8d2a4c8a,
    0xfffa3942,0x8771f681,0x6d9d6122,0xfde5380c,0xa4beea44,0x4bdecfa9,0xf6bb4b60,0xbebfbc70,
    0x289b7ec6,0xeaa127fa,0xd4ef3085,0x04881d05,0xd9d4d039,0xe6db99e5,0x1fa27cf8,0xc4ac5665,
    0xf4292244,0x432aff97,0xab9423a7,0xfc93a039,0x655b59c3,0x8f0ccc92,0xffeff47d,0x85845dd1,
    0x6fa87e4f,0xfe2ce6e0,0xa3014314,0x4e0811a1,0xf7537e82,0xbd3af235,0x2ad7d2bb,0xeb86d391
  ]);
  const S = [
    7,12,17,22, 7,12,17,22, 7,12,17,22, 7,12,17,22,
    5,9,14,20, 5,9,14,20, 5,9,14,20, 5,9,14,20,
    4,11,16,23, 4,11,16,23, 4,11,16,23, 4,11,16,23,
    6,10,15,21, 6,10,15,21, 6,10,15,21, 6,10,15,21
  ];

  const dv = new DataView(withOne.buffer);
  for (let i = 0; i < withOne.length; i += 64) {
    let AA = a, BB = b, CC = c, DD = d;
    const X = new Int32Array(16);
    for (let j = 0; j < 16; j++) X[j] = dv.getInt32(i + j*4, true);

    // Rounds
    for (let j = 0; j < 64; j++) {
      let f: number, g: number;
      if (j < 16) { f = (b & c) | (~b & d); g = j; }
      else if (j < 32) { f = (d & b) | (~d & c); g = (5*j + 1) % 16; }
      else if (j < 48) { f = b ^ c ^ d; g = (3*j + 5) % 16; }
      else { f = c ^ (b | ~d); g = (7*j) % 16; }
      const tmp = d;
      d = c;
      c = b;
      b = (b + rotl((a + f + X[g] + K[j]) | 0, S[j])) | 0;
      a = tmp;
    }

    a = (a + AA) | 0;
    b = (b + BB) | 0;
    c = (c + CC) | 0;
    d = (d + DD) | 0;
  }
  return (toHex(a) + toHex(b) + toHex(c) + toHex(d)).toLowerCase();
}

async function upsertMember(env: Env, item: any) {
  const [ , dc ] = env.MAILCHIMP_API_KEY.split('-');
  if (!dc) throw new Error('MAILCHIMP_API_KEY must include data centre suffix (e.g. us21-xxxx)');
  const hash = md5LowerHex(item.email);
  const url = `https://${dc}.api.mailchimp.com/3.0/lists/${env.MAILCHIMP_LIST_ID}/members/${hash}`;

  const body = {
    email_address: item.email,
    status_if_new: "subscribed",
    status: "subscribed",
    merge_fields: { FNAME: item.firstName, LNAME: item.lastName }
  };

  const res = await fetch(url, {
    method: "PUT",
    headers: {
      "authorization": "Basic " + btoa(`any:${env.MAILCHIMP_API_KEY}`),
      "content-type": "application/json"
    },
    body: JSON.stringify(body)
  });

  if (res.status === 429 || res.status >= 500) {
    console.warn(`mc_send retryable status=${res.status}`);
    throw new Error(`retryable ${res.status}`);
  }
  if (!res.ok) console.warn(`mc_send nonretryable status=${res.status}`);
}

export default {
  async queue(batch: MessageBatch<any>, env: Env) {
    const msgs = batch.messages;
    for (let i=0; i<msgs.length; i+=GROUP_SIZE) {
      const group = msgs.slice(i, i+GROUP_SIZE);
      await Promise.allSettled(group.map(async m => {
        const body = m.body;
        try {
          await upsertMember(env, body);
          m.ack();
        } catch (err) {
          const currentAttempts = m.attempts; // number of prior attempts recorded by the platform
          const nextAttempt = currentAttempts + 1;
          if (nextAttempt >= MAX_ATTEMPTS_BEFORE_DLQ) {
            console.warn(`mc_send dlq attempt=${nextAttempt}`);
            await env.DLQ.send({ original: body, error: String((err as any)?.message ?? err), failedAt: Date.now(), attempts: nextAttempt });
            m.ack();
          } else {
            const delay = Math.min(300, Math.pow(2, currentAttempts) * BASE_BACKOFF_SECONDS);
            m.retry({ delaySeconds: delay });
          }
        }
      }));
      if (i + GROUP_SIZE < msgs.length) await sleep(INTER_GROUP_PAUSE_MS);
    }
  }
};