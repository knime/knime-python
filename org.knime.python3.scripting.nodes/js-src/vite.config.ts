// eslint-disable-next-line spaced-comment
/// <reference types="vitest" />
import { fileURLToPath, URL } from "node:url";

import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import vueJsx from "@vitejs/plugin-vue-jsx";
import monacoEditorPlugin from "vite-plugin-monaco-editor";
import svgLoader from "vite-svg-loader";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    vueJsx(),
    monacoEditorPlugin({
      languageWorkers: ["editorWorkerService"],
    }),
    svgLoader(),
  ],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
      "monaco-editor":
        process.env.NODE_ENV === "test"
          ? fileURLToPath(
              new URL("./src/__mocks__/monaco-editor", import.meta.url),
            )
          : "monaco-editor", // We mock monaco in the test environment
    },
  },
  base: "./",
  test: {
    include: ["src/**/__tests__/**/*.test.{js,mjs,cjs,ts,mts,cts,jsx,tsx}"],
    exclude: ["**/node_modules/**", "**/dist/**", "webapps-common/**"],
    environment: "jsdom",
    reporters: ["default"],
    root: fileURLToPath(new URL("./", import.meta.url)),
    deps: {
      registerNodeLoader: true, // TODO: This option is deprecated. Replace it with a proper solution
    },
    coverage: {
      provider: "v8",
      all: true,
      exclude: [
        "coverage/**",
        "dist/**",
        "webapps-common/**",
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
