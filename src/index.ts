import { app, InvocationContext } from "@azure/functions";

/* ======= Configuration (env) ======= */
const DOWNSTREAM_URL = process.env.DOWNSTREAM_FUNCTION_URL ?? "";
const DOWNSTREAM_KEY = process.env.DOWNSTREAM_FUNCTION_KEY ?? "";

// Non-2xx: throw (to re-queue) or just log-and-continue (dev)
const FAIL_ON_NON_2XX = /^(1|true|yes)$/i.test(process.env.FAIL_ON_NON_2XX ?? "true");

// Retry settings
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 3);
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS ?? 8000);
const RETRY_BASE_MS = Number(process.env.RETRY_BASE_MS ?? 400);
// Which HTTP status codes are considered transient (retryable)
const RETRY_STATUSES = new Set(
    (process.env.RETRY_STATUS_CODES ?? "408,429,500,502,503,504")
        .split(",")
        .map(s => Number(s.trim()))
        .filter(n => !Number.isNaN(n))
);

/* ======= Helpers ======= */

function getBlobSize(payload: unknown): number {
    if (payload == null) return 0;
    if (typeof payload === "string") return Buffer.byteLength(payload);
    if (payload instanceof Uint8Array) return payload.length;
    try { return Buffer.byteLength(JSON.stringify(payload)); } catch { return 0; }
}

function sanitizeName(name: string): string {
    // Avoid leading slashes that can break matching or downstream routes.
    return name.startsWith("/") ? name.slice(1) : name;
}

function parseFromContext(ctx: InvocationContext) {
    const blobTrigger = String(ctx.triggerMetadata?.blobTrigger ?? "");
    // blobTrigger format: "<container>/<path...>/<file>"
    if (blobTrigger && blobTrigger.includes("/")) {
        const firstSlash = blobTrigger.indexOf("/");
        const container = blobTrigger.slice(0, firstSlash);
        const name = blobTrigger.slice(firstSlash + 1);  // full path inside container
        return { container, name };
    }

    // Fallback to uri if needed
    const uri = String(ctx.triggerMetadata?.uri ?? "");
    if (uri) {
        try {
            const u = new URL(uri);
            // path is like: /devstoreaccount1/<container>/<name...>
            const parts = u.pathname.replace(/^\/+/, "").split("/");
            // parts[0] = devstoreaccount1
            const container = parts[1] ?? "";
            const name = parts.slice(2).join("/");
            return { container, name };
        } catch { /* ignore */ }
    }

    return { container: "", name: "" };
}


async function postWithRetry(
    url: string,
    body: unknown,
    headers: Record<string, string>,
    context: InvocationContext
): Promise<Response> {
    let attempt = 0;
    let lastResp: Response | undefined;

    while (attempt <= MAX_RETRIES) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

        try {
            const resp = await fetch(url, {
                method: "POST",
                headers,
                body: JSON.stringify(body),
                signal: controller.signal
            });
            clearTimeout(timer);

            if (resp.ok) return resp;

            // Non-2xx — decide whether to retry
            if (RETRY_STATUSES.has(resp.status) && attempt < MAX_RETRIES) {
                const backoff = RETRY_BASE_MS * Math.pow(2, attempt) + Math.floor(Math.random() * 150);
                context.log(`Downstream ${resp.status}; retrying in ${backoff}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
                await new Promise(r => setTimeout(r, backoff));
                attempt++;
                lastResp = resp;
                continue;
            }

            // Non-retryable or out of retries
            return resp;
        } catch (err: any) {
            clearTimeout(timer);
            const isAbort = err?.name === "AbortError";
            const reason = isAbort ? `timeout (${TIMEOUT_MS}ms)` : (err?.message ?? String(err));

            if (attempt < MAX_RETRIES) {
                const backoff = RETRY_BASE_MS * Math.pow(2, attempt) + Math.floor(Math.random() * 150);
                context.log(`Downstream error: ${reason}; retrying in ${backoff}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
                await new Promise(r => setTimeout(r, backoff));
                attempt++;
                continue;
            }

            // Give up after retries by rethrowing to caller
            throw new Error(`Downstream request failed after ${MAX_RETRIES} retries: ${reason}`);
        }
    }

    // Shouldn’t hit here, but TypeScript likes a return/throw
    return lastResp!;
}

/* ======= Handler ======= */
export async function BlobWatcher(blob: unknown, context: InvocationContext): Promise<void> {
    // Validate configuration early
    if (!DOWNSTREAM_URL) {
        context.log("Config error: DOWNSTREAM_FUNCTION_URL is not set.");
        // Throw so the message is retried and visible; change to 'return' if you’d rather not poison in dev
        throw new Error("Missing DOWNSTREAM_FUNCTION_URL");
    }

    /*
    const rawName = String(context.triggerMetadata?.name ?? "");
    const container = String(context.triggerMetadata?.containerName ?? "");
    context.log("--------------------------------");
    context.log(context);
    context.log("--------------------------------");
    const name = sanitizeName(rawName);
    */
    const { container, name } = parseFromContext(context);
    const size = getBlobSize(blob);

    // Basic payload
    const payload = { Container: container, Name: name, Size: size };

    const headers: Record<string, string> = { "content-type": "application/json" };
    if (DOWNSTREAM_KEY) headers["x-functions-key"] = DOWNSTREAM_KEY;

    context.log(`New blob detected: ${container}/${name} (${size} bytes)`);
    context.log(`Posting to downstream ${DOWNSTREAM_URL}`);

    // Send with retry/timeout
    let resp: Response;
    try {
        resp = await postWithRetry(DOWNSTREAM_URL, payload, headers, context);
    } catch (err: any) {
        // Network/timeout after all retries: rethrow to re-queue
        context.log(`Downstream final error: ${err?.message ?? String(err)}`);
        throw err;
    }

    // Log the body only if small (avoid huge outputs)
    const text = await resp.text().catch(() => "");
    context.log(`Downstream returned ${resp.status}: ${text.slice(0, 2_000)}`);

    // Decide whether a non-2xx should fail the invocation (poison -> retry)
    if (!resp.ok && FAIL_ON_NON_2XX) {
        throw new Error(`Downstream call failed with ${resp.status}`);
    }
}

/* ======= Registration ======= */
app.storageBlob("BlobWatcher", {
    // For Azure prod, set your real container & prefer 'EventGrid'.
    // For Azurite dev, keep 'LogsAndContainerScan'.
    path: "my-container/{name}",
    connection: "InputStorage",
    source: process.env.AZURE_FUNCTIONS_ENVIRONMENT === "Development"
        ? "LogsAndContainerScan"
        : "EventGrid",
    handler: BlobWatcher
});
