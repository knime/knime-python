import { createApp } from "vue";
import App from "./components/App.vue";
import "./python-scripting-service"; // to make sure that the scripting-service is initialized
import { Consola, BrowserReporter, LogLevel } from "consola";

const setupConsola = () => {
  const consola = new Consola({
    level: import.meta.env.DEV ? LogLevel.Trace : LogLevel.Error,
    reporters: [new BrowserReporter()],
  });
  const globalObject = typeof global === "object" ? global : window;

  // @ts-expect-error
  globalObject.consola = consola;
};

setupConsola();

const app = createApp(App);

app.mount("#app");
