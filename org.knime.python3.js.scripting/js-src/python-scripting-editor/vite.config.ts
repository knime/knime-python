import { fileURLToPath, URL } from 'node:url';
import { defineConfig } from 'vite';

import vue from '@vitejs/plugin-vue';
import svgLoader from 'vite-svg-loader';
import monacoEditorPlugin from 'vite-plugin-monaco-editor';

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [
        vue(),
        svgLoader(),
        monacoEditorPlugin({
            languageWorkers: ['editorWorkerService'], // TODO check
        }),
    ],
    resolve: {
        alias: {
            '@': fileURLToPath(new URL('./src', import.meta.url)),
            '@@': fileURLToPath(new URL('.', import.meta.url)),
            path: 'path-browserify',
        },
        dedupe: [
            'vue',
        ],
    },
    envPrefix: 'KNIME_',
    base: './',
});
