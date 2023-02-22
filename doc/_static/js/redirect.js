/*
 * Redirects for old versions of the documentation
 */
const redirects = {
  /* Root */
  "#contents": "/index.html",

  /* Python Script API */
  "#python-script-api": "/script-api.html",
  "#inputs-and-outputs": "/script-api.html#inputs-and-outputs",
  "#classes": "/script-api.html#classes",
  "#views": "/script-api.html#views",

  /* Python Extension Development */
  "#python-extension-development-labs": "/extension-development.html",
  "#nodes": "/extension-development.html#nodes",
  "#decorators": "/extension-development.html#decorators",
  "#parameters": "/extension-development.html#parameters",
  "#tables": "/extension-development.html#tables",
  "#data-types": "/extension-development.html#data-types",

  /* Deprecated Python Script API */
  "#deprecated-python-script-api": "/deprecated-script-api.html",
  "#id3": "/deprecated-script-api.html#inputs-and-outputs",
  "#factory-methods": "/deprecated-script-api.html#factory-methods",
  "#id4": "/deprecated-script-api.html#classes",
};

function doRedirect(versionString) {
  const target = redirects[window.location.hash];
  if (target) {
    window.location.replace(window.location.origin + versionString + target);
  }
}

const p = window.location.pathname;
if (p === "/en/latest/" || p === "/en/latest/index.html") {
  // Redirect if there is a hash
  doRedirect("/en/latest")
} else if (p === "/en/stable/" || p === "/en/stable/index.html") {
  // Redirect if there is a hash
  doRedirect("/en/stable")
}
/* else:
 * No redirect necessary because we are on a specific version
 * or on a file that did not exist before
 */
