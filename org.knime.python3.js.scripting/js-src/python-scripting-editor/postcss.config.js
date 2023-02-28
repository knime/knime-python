const { preset } = require('webapps-common/webpack/webpack.postcss.config');
module.exports = {
    plugins: {
        'postcss-preset-env': preset,
    },
};
