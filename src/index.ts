export interface Env {
  MAILCHIMP_API_KEY: string;  // secret
  MAILCHIMP_LIST_ID: string;  // secret
}

async function pushToMailchimp(env: Env, item: any) {
  const apiKey = env.MAILCHIMP_API_KEY;
  const listId = env.MAILCHIMP_LIST_ID;
  if (!apiKey || !listId) return;

  const [, dc] = apiKey.split("-");
  if (!dc) throw new Error("MAILCHIMP_API_KEY must include data centre suffix (e.g. us21-xxxx)");
  const url = `https://${dc}.api.mailchimp.com/3.0/lists/${listId}/members`;

  // Optional: idempotency using Mailchimp’s member hash (md5 of lowercase email) would use PUT.
  // For simplicity we POST; switch to PUT /members/{subscriber_hash} if you want strong idempotency.
  const body = {
    email_address: item.email,
    status_if_new: "subscribed",
    status: "subscribed",
    merge_fields: { FNAME: item.firstName, LNAME: item.lastName }
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "authorization": "Basic " + btoa(`anystring:${apiKey}`),
      "content-type": "application/json"
    },
    body: JSON.stringify(body)
  });

  if (res.status === 429 || res.status >= 500) {
    // Retryable — let Queues redeliver with back-off
    const txt = await res.text();
    throw new Error(`Mailchimp rate/5xx: ${res.status} ${txt}`);
  }

  if (!res.ok) {
    // Non-retryable 4xx — log and drop
    const txt = await res.text();
    console.warn("Mailchimp non-retryable", res.status, txt);
  }
}

export default {
  // No fetch() needed unless you also want a health endpoint
  async queue(batch: MessageBatch<any>, env: Env, ctx: ExecutionContext) {
    // Sequential is safest for rate limits; replace with micro-batching if needed
    for (const msg of batch.messages) {
      try {
        await pushToMailchimp(env, msg.body);
        msg.ack();
      } catch (err) {
        console.warn("Queue processing error", (err as any)?.message || err);
        msg.retry(); // platform handles exponential back-off
      }
    }
  }
};