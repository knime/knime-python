import "@/__mocks__/browser-mock-services";

import { createApp } from "vue";
import { Consola, LogLevels } from "consola";

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

const app = createApp(App);

app.mount("#app");
