import { createApp } from 'vue';
import consola from 'consola';

import App from './App.vue';

(window as any).consola = consola.create({ // eslint-disable-line
    level: 4 // TODO: make configurable
});


const app = createApp(App);

app.mount('#app');
