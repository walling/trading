import { readFileSync } from "fs";

//-- We use readFileSync with static strings in order to track the files automatically --
// Whenever a markdown file is updated, Parcel sends an update using hot reload to the browser.
const content = {
  documents: {
    "data-models.md": readFileSync("../documents/data-models.md", "utf8"),
    "resources.md": readFileSync("../documents/resources.md", "utf8"),
  },
  "markdown-viewer": {
    "readme.md": readFileSync("../markdown-viewer/readme.md", "utf8"),
  },
  "readme.md": readFileSync("../readme.md", "utf8"),
};

export default content;
