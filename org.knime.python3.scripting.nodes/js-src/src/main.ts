import { createApp } from "vue";
import { Consola, LogLevels } from "consola";

import { init, initMocked } from "@knime/scripting-editor";
import { LoadingApp } from "@knime/scripting-editor/loading";

import App from "@/components/App.vue";

const setupConsola = () => {
  const consola = new Consola({
    level: import.meta.env.DEV ? LogLevels.trace : LogLevels.error,
  });
  const globalObject = typeof global === "object" ? global : window;

  // @ts-expect-error TODO how to tell TS that consola is a global?
  globalObject.consola = consola;
};

setupConsola();

// Show loading app while initializing
const loadingApp = createApp(LoadingApp);
loadingApp.mount("#app");

// Initialize application (e.g., load initial data, set up services)
if (import.meta.env.MODE === "development.browser") {
  // Mock the initial data and services
  initMocked((await import("@/__mocks__/browser-mock-services")).default);
} else {
  await init();
}

// Unmount loading app and mount the main app
loadingApp.unmount();
createApp(App).mount("#app");
