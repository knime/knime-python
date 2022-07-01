const MonacoWebpackPlugin = require('monaco-editor-webpack-plugin');
const path = require('path');
const { defineConfig } = require('@vue/cli-service');

module.exports = defineConfig({
    transpileDependencies: true,
    publicPath: './',
    configureWebpack: {
        plugins: [
            new MonacoWebpackPlugin({
                filename: 'js/[name].worker.js',
                languages: ['python']
            })
        ]
    },
    chainWebpack: (config) => {
    // allow tilde imports
        config.resolve.alias.set('~', path.resolve(__dirname));

        config.resolve.alias.set(
            'webapps-common',
            path.resolve(__dirname, 'webapps-common')
        );
    }
});
