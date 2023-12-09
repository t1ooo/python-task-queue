import {
  html,
  render,
  useState,
} from "https://esm.sh/htm@3.1.1/preact/standalone";

class ErrorResponse extends Error {
  constructor(message) {
    super(message);
    this.name = "UserException";
  }
}

async function request(url, options) {
  const resp = await fetch(url, options);
  const resp_data = await resp.json();
  if ("error" in resp_data) {
    throw new ErrorResponse(resp_data.error);
  }
  if (!("data" in resp_data)) {
    throw Error("bad response schema" + JSON.stringify(resp_data));
  }
  return resp_data.data;
}

async function taskStatus(task_id) {
  return await request("/task-status", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ id: task_id }),
  });
}

async function uploadFile(file) {
  const formData = new FormData();
  formData.append("file", file);
  return await request("/upload-pdf", {
    method: "POST",
    body: formData,
  });
}

const FileUploadForm = () => {
  const [selectedFile, setSelectedFile] = useState(null);
  const [downloadLink, setDownloadLink] = useState(null);
  const [error, setError] = useState(null);
  const [status, setStatus] = useState(null);

  const reset = () => {
    setError(null);
    setStatus(null);
    setDownloadLink(null);
  };

  const STATUS = {
    SUCCESS: "SUCCESS",
    FAILURE: "FAILURE",
    IN_PROGRESS: "IN_PROGRESS",
  };

  const POLL_DELAY = 5 * 1000;

  const pollTaskStatus = async (task_id) => {
    try {
      console.log(task_id);
      const status = await taskStatus(task_id);
      setStatus(status);
      if (status == STATUS.SUCCESS) {
        setDownloadLink("/download/" + task_id);
      } else if (status == STATUS.FAILURE) {
        setError("Fail to extract text from pdf");
      } else {
        setTimeout(() => pollTaskStatus(task_id), POLL_DELAY);
      }
    } catch (error) {
      if (error instanceof ErrorResponse) {
        setError(error.message);
      }
      console.error(error);
      setError("Something went wrong");
    }
  };

  const handleFileChange = (event) => {
    reset();

    setSelectedFile(event.target.files[0]);
  };

  const handleSubmit = async (event) => {
    event.preventDefault();

    reset();

    try {
      console.log(selectedFile);
      const task_id = await uploadFile(selectedFile);
      setStatus(STATUS.IN_PROGRESS);
      setTimeout(() => pollTaskStatus(task_id), POLL_DELAY);
    } catch (error) {
      if (error instanceof ErrorResponse) {
        setError(error.message);
      }
      console.error(error);
      setError("Something went wrong");
    }
  };

  const handleDownload = () => {
    downloadLink && window.open(downloadLink);
  };

  return html`
    <form onSubmit="${handleSubmit}">
      <div class="file-upload">
        <label htmlFor="file">Select PDF:</label>
        <input
          type="file"
          accept=".pdf"
          onChange="${handleFileChange}"
        />
        <button type="submit" disabled="${!selectedFile}">Extract Text</button>
      </div>
    </form>
    ${status == STATUS.IN_PROGRESS &&
    html`<div class="loader-container">
      <div class="loader"></div>
    </div>`}
    ${downloadLink &&
    html`<button onClick="${handleDownload}">Download Result</button>`}
    ${error && html`<div class="error">ERROR: ${error}</div>`}
  `;
};

render(html`<${FileUploadForm} />`, document.getElementById("app"));
