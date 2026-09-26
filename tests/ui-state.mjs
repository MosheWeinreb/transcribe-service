// Run with node tests/ui-state.mjs. Checks UI state without a browser or microphone.
import assert from "node:assert/strict";
import { File } from "node:buffer";
import { readFileSync } from "node:fs";
import { setImmediate } from "node:timers/promises";
import { runInNewContext } from "node:vm";

const elements = new Map();
function element(id) {
  if (!elements.has(id)) {
    elements.set(id, {
      disabled: false,
      hidden: false,
      listeners: {},
      dataset: {},
      classList: { add() {}, remove() {}, toggle() {} },
      addEventListener(type, callback) {
        this.listeners[type] = callback;
      },
      setAttribute(name, value) { this[name] = value; },
      removeAttribute(name) { delete this[name]; },
      pause() {},
    });
  }
  return elements.get(id);
}
let grantPermission,
  denyPermission,
  activeRecorder,
  copiedText,
  allowUpload = false,
  stopped = false;
let upload, poll, jobStatus = "queued";
class UploadRequest {
  constructor() {
    upload = this;
    this.listeners = {};
    this.upload = { listeners: {}, addEventListener(type, callback) { this.listeners[type] = callback; } };
  }
  addEventListener(type, callback) { this.listeners[type] = callback; }
  open(method, url) { assert.equal(method, "POST"); assert.ok(url.startsWith("/api/jobs?filename=")); }
  setRequestHeader(name, value) { assert.equal(name, "Content-Type"); assert.equal(value, "application/octet-stream"); }
  send(file) { assert.equal(allowUpload, true); assert.ok(file instanceof File); }
  complete(status, body) {
    this.status = status;
    this.responseText = JSON.stringify(body);
    this.listeners.load();
  }
}
const transcript = "A completed transcription.";
class Recorder {
  static isTypeSupported() {
    return true;
  }
  constructor() {
    activeRecorder = this;
    this.listeners = {};
    this.mimeType = "audio/webm";
  }
  addEventListener(type, callback) {
    this.listeners[type] = callback;
  }
  start() {
    this.state = "recording";
  }
  stop() {
    this.state = "inactive";
  }
}
runInNewContext(
  readFileSync(new URL("../web/app.js", import.meta.url), "utf8"),
  {
    document: { getElementById: element },
    window: { MediaRecorder: Recorder, addEventListener() {} },
    MediaRecorder: Recorder,
    XMLHttpRequest: UploadRequest,
    Response,
    navigator: {
      clipboard: {
        writeText: async (text) => { copiedText = text; },
      },
      mediaDevices: {
        getUserMedia: () =>
          new Promise((resolve, reject) => {
            grantPermission = resolve;
            denyPermission = reject;
          }),
      },
    },
    localStorage: { getItem: () => null },
    URL: { createObjectURL: () => "blob:preview", revokeObjectURL() {} },
    File,
    Blob,
    setInterval: () => 1,
    clearInterval() {},
    clearTimeout() {},
    setTimeout(callback) { poll = callback; return 1; },
    fetch: async (url) => {
      if (url !== "/api/config") {
        assert.equal(allowUpload, true, "No upload should start while recording");
        assert.ok(url.startsWith("/api/jobs"));
        return {
          ok: true,
          json: async () => ({
            id: "test-job",
            filename: "recording.webm",
            status: jobStatus,
            transcript: jobStatus === "succeeded" ? transcript : null,
            created_at: new Date().toISOString(),
            finished_at: new Date().toISOString(),
          }),
        };
      }
      return {
        ok: true,
        json: async () => ({
          max_upload_bytes: 100,
          max_filename_length: 20,
          max_duration_seconds: 60,
          retention_seconds: 3600,
        }),
      };
    },
  },
);
await setImmediate(); // Finish loading the public settings.

const select = (file) =>
  element("file-input").listeners.change({ target: { files: [file] } });
const valid = new File(["audio"], "valid.wav", { type: "audio/wav" });
select(valid);
assert.equal(element("submit").disabled, false);
select(new File(["x".repeat(101)], "large.wav"));
assert.equal(
  element("submit").disabled,
  true,
  "An invalid replacement must clear the old file",
);
select(valid);
select(new File(["audio"], "x".repeat(21)));
assert.equal(element("submit").disabled, true);

select(valid);
const starting = element("record-button").listeners.click();
assert.equal(element("submit").disabled, true);
assert.equal(element("upload-mode").disabled, true);
await element("submit").listeners.click();
grantPermission({
  getTracks: () => [
    {
      stop() {
        stopped = true;
      },
    },
  ],
});
await starting;
assert.equal(
  element("record-button").disabled,
  false,
  "Stop must remain available",
);
element("dropzone").listeners.drop({
  preventDefault() {},
  dataTransfer: { files: [valid] },
});
assert.equal(
  element("submit").disabled,
  true,
  "Dropping a file cannot interrupt recording",
);
await element("record-button").listeners.click();
assert.equal(stopped, true);
assert.equal(
  element("record-button").disabled,
  true,
  "Wait for final recording data",
);
activeRecorder.listeners.dataavailable({ data: new Blob(["recorded audio"]) });
activeRecorder.listeners.stop();
assert.equal(element("file-name").textContent, "recording.webm");
assert.equal(element("submit").disabled, false);
assert.equal(element("upload-mode").disabled, false);

const denied = element("record-button").listeners.click();
denyPermission(new Error("Permission denied"));
await denied;
assert.equal(element("record-button").disabled, false);
assert.equal(
  element("submit").disabled,
  false,
  "Permission denial preserves the selected file",
);
assert.match(element("error").textContent, /Microphone access was unavailable/);
allowUpload = true;
const submitted = element("submit").listeners.click();
assert.equal(element("activity").value, 0);
upload.upload.listeners.progress({ lengthComputable: false });
assert.equal(element("activity").value, undefined);
upload.upload.listeners.progress({ lengthComputable: true, loaded: 7, total: 14 });
assert.equal(element("activity").value, 50);
assert.match(element("status-message").textContent, /50%/);
upload.upload.listeners.load();
assert.equal(element("activity").value, 100);
assert.equal(element("status-badge").textContent, "Uploading", "Sent bytes are not server acceptance");
upload.complete(202, { id: "test-job" });
await submitted;
assert.equal(element("status-badge").textContent, "Queued");
assert.equal(element("activity").value, undefined, "Queue wait must not show a fake percentage");
assert.equal(element("step-queued")["aria-current"], "step");
jobStatus = "processing";
await poll();
assert.equal(element("status-badge").textContent, "Transcribing");
assert.equal(element("submit").disabled, true);
jobStatus = "succeeded";
await poll();
assert.equal(element("activity").hidden, true);
assert.equal(element("transcript").textContent, transcript);
assert.equal(element("result-actions").hidden, false);
assert.equal(element("submit").textContent, "Transcribe recording");
assert.equal(element("submit").disabled, false);
await element("copy").listeners.click();
assert.equal(copiedText, transcript);
const rejected = element("submit").listeners.click();
assert.equal(element("transcript").hidden, true, "Old results must not appear under a new upload");
upload.complete(413, { error: { message: "Upload too large." } });
await rejected;
assert.equal(element("error").textContent, "Upload too large.");
assert.equal(element("submit").disabled, false);
assert.equal(element("activity").hidden, true);
const disconnected = element("submit").listeners.click();
upload.listeners.error();
await disconnected;
assert.match(element("error").textContent, /connection lost/);
assert.equal(element("submit").disabled, false);
console.log(
  "UI checks passed: recording, permissions, upload progress, job stages, failures, and copy.",
);
