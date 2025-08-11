/// <reference types="@cloudflare/workers-types" />
export interface Env {
  MAILCHIMP_API_KEY: string;
  MAILCHIMP_LIST_ID: string;
  DLQ: Queue; // dead-letter queue producers
}

const GROUP_SIZE = 10;
const INTER_GROUP_PAUSE_MS = 250;
const MAX_ATTEMPTS_BEFORE_DLQ = 5;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function md5HexLower(s: string) {
  const d = await crypto.subtle.digest('MD5', new TextEncoder().encode(s.toLowerCase()));
  return [...new Uint8Array(d)].map(b=>b.toString(16).padStart(2,'0')).join('');
}

function bumpAttempt(body: any) { return { ...body, attempt: (body?.attempt ?? 0) + 1 }; }

async function upsertMember(env: Env, item: any) {
  const [ , dc ] = env.MAILCHIMP_API_KEY.split('-');
  if (!dc) throw new Error('MAILCHIMP_API_KEY must include data centre suffix (e.g. us21-xxxx)');
  const hash = await md5HexLower(item.email);
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
      const results = await Promise.allSettled(group.map(async m => {
        const body = bumpAttempt(m.body);
        try {
          await upsertMember(env, body);
          m.ack();
        } catch (err) {
          if (body.attempt >= MAX_ATTEMPTS_BEFORE_DLQ) {
            console.warn(`mc_send dlq attempt=${body.attempt}`);
            await env.DLQ.send({ original: body, error: String((err as any)?.message ?? err), failedAt: Date.now() });
            m.ack();
          } else {
            m.body = body; // keep attempt count
            m.retry();
          }
        }
      }));
      if (i + GROUP_SIZE < msgs.length) await sleep(INTER_GROUP_PAUSE_MS);
    }
  }
};