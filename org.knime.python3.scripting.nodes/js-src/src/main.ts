import { createApp } from "vue";
import { createPinia } from "pinia";
import App from "./App.vue";
import "./python-scripting-service"; // to make sure that the scripting-service is initialized

const app = createApp(App);

app.use(createPinia());

app.mount("#app");
