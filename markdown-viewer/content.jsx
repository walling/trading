import { readFileSync } from "fs";

//-- Directory/file tree structure of markdown files --
// We use readFileSync with static strings in order to track the files automatically.
// Whenever a file is updated, Parcel should send a hot reload update to the browser.
// Please try to follow the directory structure of the repo.
const content = {
  documents: {
    "data-models.md": readFileSync("../documents/data-models.md", "utf8"),
    "resources.md": readFileSync("../documents/resources.md", "utf8"),
    "turbulence-and-trading.md": readFileSync(
      "../documents/turbulence-and-trading.md",
      "utf8"
    ),
  },
  lib: {
    dataset: {
      "readme.md": readFileSync("../lib/dataset/readme.md", "utf8"),
    },
  },
  "markdown-viewer": {
    "readme.md": readFileSync("../markdown-viewer/readme.md", "utf8"),
  },
  planning: {
    "todo-troels.md": readFileSync("../planning/todo-troels.md", "utf8"),
    "todo-bjarke.md": readFileSync("../planning/todo-bjarke.md", "utf8"),
  },
  "readme.md": readFileSync("../readme.md", "utf8"),
};

export default content;
