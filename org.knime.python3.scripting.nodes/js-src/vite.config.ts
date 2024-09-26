// eslint-disable-next-line spaced-comment
/// <reference types="vitest" />
import { fileURLToPath, URL } from "node:url";

import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import monacoEditorPlugin, {
  type IMonacoEditorOpts,
} from "vite-plugin-monaco-editor";
import svgLoader from "vite-svg-loader";
// @ts-ignore
import { svgoConfig } from "@knime/styles/config/svgo.config";

// Hack because default export of vite-plugin-monaco-editor is wrong (and does not fit types)
// https://github.com/vdesjs/vite-plugin-monaco-editor/issues/21
const monacoEditorPluginDefault = (monacoEditorPlugin as any).default as (
  options: IMonacoEditorOpts,
) => any;

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    monacoEditorPluginDefault({
      languageWorkers: ["editorWorkerService"],
    }),
    svgLoader({ svgoConfig }),
  ],
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  optimizeDeps: { exclude: ["@knime/scripting-editor"] },
  base: "./",
  test: {
    setupFiles: [
      fileURLToPath(new URL("./src/test-setup/setup.ts", import.meta.url)),
    ],
    include: ["src/**/__tests__/**/*.test.{js,mjs,cjs,ts,mts,cts,jsx,tsx}"],
    exclude: ["**/node_modules/**", "**/dist/**"],
    environment: "jsdom",
    reporters: ["default"],
    root: fileURLToPath(new URL("./", import.meta.url)),
    server: { deps: { inline: ["@knime/scripting-editor"] } },
    pool: "threads",
    alias: {
      "monaco-editor": fileURLToPath(
        new URL("./src/__mocks__/monaco-editor", import.meta.url),
      ),
    },
    coverage: {
      provider: "v8",
      all: true,
      exclude: [
        "coverage/**",
        "dist/**",
        "lib/**",
        "**/*.d.ts",
        "**/__tests__/**",
        "**/__mocks__/**",
        "test-setup/**",
        "**/{vite,vitest,postcss,lint-staged}.config.{js,cjs,mjs,ts}",
        "**/.{eslint,prettier,stylelint}rc.{js,cjs,yml}",
      ],
      reporter: ["html", "text", "lcov"],
    },
  },
});
