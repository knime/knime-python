module.exports = {
  extends: ["@knime/eslint-config/stylelint/vue"],
  rules: {
    "csstools/value-no-unknown-custom-properties": [
      true,
      {
        importFrom: [
          "node_modules/@knime/styles/css/variables/index.css",
          "node_modules/@knime/kds-styles/dist/tokens/css/_variables.css",
        ],
      },
    ],
  },
};
