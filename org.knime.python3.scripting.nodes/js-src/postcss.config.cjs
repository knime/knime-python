const { preset } = require("@knime/styles/config/postcss.config.cjs");

module.exports = {
  plugins: {
    "postcss-preset-env": preset,
  },
};
