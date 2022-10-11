import { fileURLToPath, URL } from 'node:url';
import { defineConfig } from 'vite';

import vue from '@vitejs/plugin-vue';
import svgLoader from 'vite-svg-loader';
import monacoEditorPlugin from 'vite-plugin-monaco-editor';

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [
        vue({
            template: {
                compilerOptions: {
                    compatConfig: {
                        MODE: 2
                    }
                }
            }
        }),
        svgLoader(),
        monacoEditorPlugin({
            languageWorkers: ['editorWorkerService'] // TODO check
        })
    ],
    resolve: {
        alias: {
            // vue: '@vue/compat',
            '@': fileURLToPath(new URL('./src', import.meta.url)),
            '@@': fileURLToPath(new URL('.', import.meta.url)),
            path: 'path-browserify'
        }
    },
    envPrefix: 'KNIME_',
    base: './'
});
