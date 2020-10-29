import React from "react";
import ReactDOM from "react-dom";
import { LocationProvider } from "@reach/router";
import content from "./content";
import Viewer from "./viewer";

//-- Use hot reloading --
if (module.hot) module.hot.accept();

async function app() {
  //-- HACK: this makes it possible to use `react-syntax-highlighter` --
  // See: https://github.com/parcel-bundler/parcel/issues/3176#issuecomment-642568855
  await import("refractor");
  const Markdown = (await import("./markdown")).default;

  ReactDOM.render(
    <LocationProvider>
      <Viewer markdownComponent={Markdown} tree={content} />
    </LocationProvider>,
    document.getElementById("root")
  );
}

app();
