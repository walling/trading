import React from "react";
import { Link, useLocation } from "@reach/router";

const DEFAULT_FILENAME = "readme.md";

function splitPath(path) {
  return path ? path.split("/") : [];
}

function getNode(node, path) {
  if (!node) return null;
  if (path.length === 0) return node;
  return getNode(node[path[0]], path.slice(1));
}

function joinPath(path) {
  return path.map((p) => `/${p}`).join("");
}

export default function Viewer({ markdownComponent, tree }) {
  const Markdown = markdownComponent;

  const location = useLocation();
  const path = splitPath(location.pathname.substring(1).replace(/\/$/, ""));
  const node = getNode(tree, path);
  const isPathFound = !!node;
  const directoryPath = typeof node === "string" ? path.slice(0, -1) : path;
  const directoryObject = getNode(tree, directoryPath) || {};
  const markdown =
    typeof node === "string" ? node : node[DEFAULT_FILENAME] || "";

  const directory = Object.keys(directoryObject)
    .map((name) => ({
      name,
      type: typeof directoryObject[name] === "string" ? "file" : "dir",
    }))
    .map(({ name, type }) => ({
      path: `${joinPath(directoryPath)}/${name}${type === "dir" ? `/` : ""}`,
      displayName: `${name}${type === "dir" ? "/" : ""}`,
      name,
      type,
    }))
    .sort(
      (a, b) => a.type.localeCompare(b.type) || a.name.localeCompare(b.name)
    );

  return (
    <React.Fragment>
      <header className="Navigation">
        {isPathFound ? (
          <nav className="container">
            <ul className="Navigation__list">
              {directoryPath.length > 0 ? (
                <li className="Navigation__item">
                  <Link
                    to={joinPath(directoryPath.slice(0, -1))}
                    className="Navigation__link"
                  >
                    <em>up one level</em>
                  </Link>
                </li>
              ) : null}
              {directory.map(({ path, displayName, type }) => (
                <li key={path} className="Navigation__item">
                  <Link to={path} className="Navigation__link">
                    {displayName}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
        ) : (
          <p>
            Not found: {path.join("/")}. <Link to="/">Go back</Link>.
          </p>
        )}
      </header>
      <main className="container">
        <Markdown markdown={markdown} />
      </main>
    </React.Fragment>
  );
}
