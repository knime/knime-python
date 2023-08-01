const { preset } = require("webapps-common/config/postcss.config");

module.exports = {
  plugins: {
    "postcss-preset-env": preset,
  },
};
