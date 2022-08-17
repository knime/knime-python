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

        config.resolve.alias.set('webapps-common', path.resolve(__dirname, 'webapps-common'));

        /* our SVG rule in webapps-common isn't compatible with VueCLI 5 / Webpack 5 */
        const svgRule = config.module.rule('svg');
        svgRule.uses.clear();
        svgRule.delete('type');
        svgRule.delete('generator');

        svgRule
            // load svgs as data-url
            .oneOf('data-url')
            .resourceQuery(/data/)
            .use('url-loader')
            .loader('url-loader')
            .end()
            .end()

            // load svgs as file imports
            .oneOf('file-import')
            .resourceQuery(/file/)
            .use('file-loader')
            .loader('file-loader')
            .options({
                name: 'assets/[name].[hash:8].[ext]'
            })
            .end()
            .end()

            // load svgs as inlined elements
            .oneOf('inline')
            .use('vue-loader')
            .loader('vue-loader')
            .end()
            .use('vue-svg-loader')
            .loader('vue-svg-loader');
    }
});
