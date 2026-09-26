"use strict";
const el = (id) => document.getElementById(id);
const terminal = new Set(["succeeded", "failed"]);
let config,
  selected,
  previewUrl,
  recorder,
  recordTimer,
  pollTimer,
  recordingStarted;
let microphone,
  recordingChunks = [],
  recordingBytes = 0,
  currentJob,
  busy = false;
let recordingFailed = false;

function showError(message = "") {
  el("error").textContent = message;
  el("error").hidden = !message;
}
function savedJob(value) {
  try {
    if (value === undefined) return localStorage.getItem("clear-job");
    if (value === null) localStorage.removeItem("clear-job");
    else localStorage.setItem("clear-job", value);
  } catch {
    /* Storage can be disabled; the current page still works. */
  }
}
function setBusy(value) {
  busy = value;
  for (const id of [
    "file-input",
    "record-button",
    "upload-mode",
    "record-mode",
    "clear-file",
  ]) {
    el(id).disabled = value;
  }
  el("submit").disabled = value || !selected || !config;
}
function selectFile(file) {
  if (busy || !config || !file) return;
  showError();
  if (file.size === 0 || file.size > config.max_upload_bytes) {
    clearFile();
    showError(
      file.size === 0
        ? "This file is empty."
        : "This file exceeds the upload limit.",
    );
    return;
  }
  if ([...file.name].length > config.max_filename_length) {
    clearFile();
    showError("Choose a file with a shorter name.");
    return;
  }
  selected = file;
  el("file-name").textContent = file.name;
  el("file-size").textContent = `${(file.size / 1024 / 1024).toFixed(2)} MiB`;
  el("selection").hidden = false;
  el("submit").disabled = false;
  if (previewUrl) URL.revokeObjectURL(previewUrl);
  previewUrl = URL.createObjectURL(file);
  el("preview").src = previewUrl;
  el("preview").hidden = !file.type.startsWith("audio/");
}
function clearFile() {
  selected = null;
  el("selection").hidden = true;
  el("preview").pause();
  el("preview").removeAttribute("src");
  el("preview").hidden = true;
  el("file-input").value = "";
  el("submit").disabled = true;
  if (previewUrl) URL.revokeObjectURL(previewUrl);
  previewUrl = null;
}
async function responseJson(response) {
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(
      data.error?.message || "The service is unavailable. Please try again.",
    );
    error.status = response.status;
    throw error;
  }
  return data;
}
function renderStage(status) {
  const badge = el("status-badge");
  badge.textContent = {
    uploading: "Uploading",
    queued: "Queued",
    processing: "Transcribing",
    succeeded: "Complete",
    failed: "Couldn’t transcribe",
  }[status];
  badge.dataset.status = status;
  const stages = ["uploading", "queued", "processing", "succeeded"];
  const current = stages.indexOf(status);
  stages.forEach((stage, index) => {
    el(`step-${stage}`).dataset.state = index < current ? "done" : "pending";
    el(`step-${stage}`).setAttribute(
      "aria-current", index === current ? "step" : "false",
    );
  });
  el("activity").hidden = terminal.has(status);
  el("activity").removeAttribute("value");
  el("activity").setAttribute("aria-label", badge.textContent);
}
function renderJob(job) {
  el("copy").textContent = "Copy text";
  el("empty-state").hidden = true;
  el("job-state").hidden = false;
  el("job-filename").textContent = job.filename;
  renderStage(job.status);
  const end = job.finished_at ? new Date(job.finished_at) : new Date();
  el("elapsed").textContent =
    `${Math.max(0, Math.floor((end - new Date(job.created_at)) / 1000))}s elapsed`;
  el("status-message").textContent = {
    queued: "Your recording is saved and waiting for an available worker.",
    processing:
      "Listening to your recording. You can leave this page and come back.",
    succeeded: job.transcript
      ? "Your words are ready."
      : "Processing finished. No speech was detected.",
    failed: job.error?.message || "This recording could not be transcribed.",
  }[job.status];
  el("transcript").textContent = job.transcript || "";
  el("transcript").hidden = !job.transcript;
  el("result-actions").hidden = !job.transcript;
  el("language").textContent = job.language
    ? `Language: ${job.language.toUpperCase()}`
    : "";
}
async function pollJob(id) {
  clearTimeout(pollTimer);
  if (currentJob !== id) return;
  try {
    const job = await responseJson(
      await fetch(`/api/jobs/${encodeURIComponent(id)}`),
    );
    if (currentJob !== id) return;
    renderJob(job);
    showError();
    if (terminal.has(job.status)) {
      setBusy(false);
      return;
    }
  } catch (error) {
    if (error.status === 404 || error.status === 422) {
      savedJob(null);
      currentJob = null;
      setBusy(false);
      renderStage("failed");
      el("status-message").textContent =
        "This job is no longer available. Submit a new recording.";
      showError(error.message);
      return;
    }
    showError("Connection interrupted. Your saved job is safe; reconnecting…");
  }
  pollTimer = setTimeout(() => pollJob(id), config.poll_interval_ms);
}
function uploadFile(file) {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.upload.addEventListener("progress", (event) => {
      if (!event.lengthComputable) {
        el("activity").removeAttribute("value");
        el("status-message").textContent = "Uploading your recording…";
        return;
      }
      const percent = Math.min(100, Math.floor((event.loaded / event.total) * 100));
      el("activity").value = percent;
      el("status-message").textContent = `Uploading your recording · ${percent}%`;
    });
    request.upload.addEventListener("load", () => {
      el("activity").value = 100;
      el("status-message").textContent =
        "Upload sent · waiting for the service to accept it.";
    });
    request.addEventListener("load", () => {
      resolve(new Response(request.responseText, { status: request.status }));
    });
    request.addEventListener("error", () => reject(new Error(
      "Upload connection lost. Check your connection before trying again.",
    )));
    request.addEventListener("abort", () => reject(new Error(
      "The upload was interrupted.",
    )));
    request.open("POST", `/api/jobs?filename=${encodeURIComponent(file.name)}`);
    request.setRequestHeader("Content-Type", "application/octet-stream");
    request.send(file);
  });
}
el("submit").addEventListener("click", async () => {
  if (!selected || busy) return;
  setBusy(true);
  showError();
  clearTimeout(pollTimer);
  currentJob = null;
  savedJob(null);
  el("empty-state").hidden = true;
  el("job-state").hidden = false;
  el("transcript").hidden = true;
  el("result-actions").hidden = true;
  el("job-filename").textContent = selected.name;
  el("elapsed").textContent = "";
  renderStage("uploading");
  el("activity").value = 0;
  el("status-message").textContent = "Uploading your recording · 0%";
  el("submit").textContent = "Uploading recording…";
  try {
    const data = await responseJson(await uploadFile(selected));
    currentJob = data.id;
    savedJob(data.id);
    renderStage("queued");
    el("status-message").textContent =
      "Your recording is saved and waiting to be transcribed.";
    await pollJob(data.id);
  } catch (error) {
    setBusy(false);
    renderStage("failed");
    el("status-badge").textContent = "Upload interrupted";
    el("status-message").textContent =
      "We couldn’t confirm this upload. Your selected recording is still here.";
    showError(error.message);
  } finally {
    el("submit").textContent = "Transcribe recording";
  }
});
el("file-input").addEventListener("change", (event) =>
  selectFile(event.target.files[0]),
);
el("clear-file").addEventListener("click", clearFile);
for (const type of ["dragenter", "dragover", "dragleave", "drop"]) {
  el("dropzone").addEventListener(type, (event) => {
    event.preventDefault();
    el("dropzone").classList.toggle(
      "dragover",
      type === "dragenter" || type === "dragover",
    );
    if (type === "drop") selectFile(event.dataTransfer.files[0]);
  });
}
for (const mode of ["upload", "record"]) {
  el(`${mode}-mode`).addEventListener("click", () => {
    if (recorder?.state === "recording") return;
    for (const item of ["upload", "record"]) {
      el(`${item}-area`).hidden = item !== mode;
      el(`${item}-mode`).classList.toggle("active", item === mode);
      el(`${item}-mode`).setAttribute("aria-pressed", String(item === mode));
    }
  });
}
function stopRecording() {
  if (recorder?.state === "recording") {
    el("record-button").disabled = true;
    recorder.stop();
  }
  microphone?.getTracks().forEach((track) => track.stop());
  clearInterval(recordTimer);
  el("record-area").classList.remove("recording");
  el("record-button").textContent = "Start recording";
}
el("record-button").addEventListener("click", async () => {
  if (recorder?.state === "recording") {
    stopRecording();
    return;
  }
  if (busy || !config) return;
  showError();
  if (!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) {
    showError(
      "Recording needs a supported browser on HTTPS or localhost. You can still upload a file.",
    );
    return;
  }
  setBusy(true);
  try {
    microphone = await navigator.mediaDevices.getUserMedia({ audio: true });
    const mimeType = [
      "audio/webm;codecs=opus",
      "audio/mp4",
      "audio/ogg;codecs=opus",
    ].find((type) => MediaRecorder.isTypeSupported(type));
    recorder = new MediaRecorder(microphone, mimeType ? { mimeType } : {});
    recordingChunks = [];
    recordingBytes = 0;
    recordingFailed = false;
    clearFile();
    recorder.addEventListener("dataavailable", (event) => {
      recordingBytes += event.data.size;
      if (recordingBytes > config.max_upload_bytes) {
        recordingFailed = true;
        showError(
          "The recording reached the upload limit. Please record a shorter clip.",
        );
        stopRecording();
        return;
      }
      recordingChunks.push(event.data);
    });
    recorder.addEventListener("stop", () => {
      stopRecording();
      setBusy(false);
      if (recordingFailed) {
        recordingChunks = [];
        return;
      }
      const extension = recorder.mimeType.includes("mp4")
        ? "m4a"
        : recorder.mimeType.includes("ogg")
          ? "ogg"
          : "webm";
      selectFile(
        new File(recordingChunks, `recording.${extension}`, {
          type: recorder.mimeType,
        }),
      );
      el("record-help").textContent = "Preview the recording, then submit it.";
    });
    recorder.addEventListener("error", () => {
      recordingFailed = true;
      stopRecording();
      showError("Recording failed. Please try again.");
    });
    recorder.start(1000);
    recordingStarted = Date.now();
    el("record-area").classList.add("recording");
    el("record-button").textContent = "Stop recording";
    el("record-help").textContent = "Recording. Press stop when finished.";
    el("record-button").disabled = false;
    el("record-time").textContent = "00:00";
    recordTimer = setInterval(() => {
      const seconds = Math.floor((Date.now() - recordingStarted) / 1000);
      el("record-time").textContent =
        `${String(Math.floor(seconds / 60)).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}`;
      if (Date.now() - recordingStarted >= config.recording_stop_ms)
        stopRecording();
    }, 250);
  } catch {
    stopRecording();
    setBusy(false);
    showError(
      "Microphone access was unavailable. Check permission, or upload a recording.",
    );
  }
});
el("copy").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(el("transcript").textContent);
    el("copy").textContent = "Copied";
  } catch {
    showError(
      "Copy was unavailable. Select the transcript to copy it manually.",
    );
  }
});
el("download").addEventListener("click", () => {
  const url = URL.createObjectURL(
    new Blob([el("transcript").textContent], { type: "text/plain;charset=utf-8" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = "transcript.txt";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
window.addEventListener("pagehide", () =>
  microphone?.getTracks().forEach((track) => track.stop()),
);
(async () => {
  try {
    config = await responseJson(await fetch("/api/config"));
    el("limits").textContent =
      `Up to ${Math.round(config.max_upload_bytes / 1024 / 1024)} MiB · ${Math.round(config.max_duration_seconds / 60)} minutes per recording`;
    el("retention").textContent =
      `Processed on this service. Files and results expire ${Math.round(config.retention_seconds / 3600)} hours after completion.`;
    const id = savedJob();
    if (id) {
      currentJob = id;
      setBusy(true);
      await pollJob(id);
    }
  } catch (error) {
    showError(error.message);
    el("limits").textContent = "Could not load settings. Please refresh.";
  }
})();
