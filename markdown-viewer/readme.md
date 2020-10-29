# Markdown viewer

You can use this tool to view markdown files in browser.

Install and run:

```bash
yarn # only first time
yarn start
```

Open the browser at <http://localhost:1234>.

## Adding markdown files

To add a new markdown file, simply add it in `content.jsx`. You need to call `readFileSync` with string values, otherwise the file can't be inlined statically.

## Files

-   `index.html`: HTML and CSS for application
-   `index.jsx`: Application setup
-   `viewer.jsx`: Viewer component
-   `markdown.jsx`: Markdown component
-   `content.jsx`: Load markdown files

## Code style

Please setup and use [Prettier](https://prettier.io) to keep the code style consistent.
