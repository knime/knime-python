// eslint-disable-next-line spaced-comment
/// <reference types="vite/client vite-svg-loader" />

interface ImportMetaEnv {
  readonly VITE_SCRIPTING_API_MOCK: "true" | "false";
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
