import { createApp } from "vue";
import { createPinia } from "pinia";
import App from "./components/App.vue";
import "./python-scripting-service"; // to make sure that the scripting-service is initialized
import { Consola, LogLevel } from "consola";

export const consola = new Consola({
  level:
    import.meta.env.VITE_SCRIPTING_API_MOCK === "true"
      ? LogLevel.Log
      : LogLevel.Silent,
});

window.consola = consola;

const app = createApp(App);

app.use(createPinia());

app.mount("#app");
