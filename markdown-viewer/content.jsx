import { readFileSync } from "fs";

//-- Directory/file tree structure of markdown files --
// We use readFileSync with static strings in order to track the files automatically.
// Whenever a file is updated, Parcel should send a hot reload update to the browser.
// Please try to follow the directory structure of the repo.
const content = {
  documents: {
    "data-models.md": readFileSync("../documents/data-models.md", "utf8"),
    "resources.md": readFileSync("../documents/resources.md", "utf8"),
  },
  "markdown-viewer": {
    "readme.md": readFileSync("../markdown-viewer/readme.md", "utf8"),
  },
  planning: {
    "todo-troels.txt": readFileSync("../planning/todo-troels.txt", "utf8"),
  },
  "readme.md": readFileSync("../readme.md", "utf8"),
};

export default content;
