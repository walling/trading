import React from "react";
import ReactMarkdown from "react-markdown";
import { InlineMath, BlockMath } from "react-katex";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { prism } from "react-syntax-highlighter/dist/esm/styles/prism";
import gfm from "remark-gfm";
import math from "remark-math";
import "katex/dist/katex.min.css"; // CSS used by react-katex

const renderers = {
  inlineMath: ({ value }) => <InlineMath math={value} />,
  math: ({ value }) => <BlockMath math={value} />,
  code: ({ language, value }) => (
    <SyntaxHighlighter style={prism} language={language} children={value} />
  ),
};

export default function Markdown({ markdown }) {
  return (
    <ReactMarkdown
      linkTarget="_blank"
      plugins={[gfm, math]}
      renderers={renderers}
      children={markdown}
    />
  );
}
