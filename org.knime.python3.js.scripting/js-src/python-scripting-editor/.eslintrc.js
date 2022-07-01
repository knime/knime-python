/* eslint-disable no-process-env */
module.exports = {
    extends: ['./webapps-common/lint/.eslintrc-vue.js', '@vue/typescript/recommended'],
    env: {
        node: true
    },
    rules: {
        'no-console': process.env.NODE_ENV === 'production' ? 'warn' : 'off',
        'no-debugger': process.env.NODE_ENV === 'production' ? 'warn' : 'off'
    }
};
