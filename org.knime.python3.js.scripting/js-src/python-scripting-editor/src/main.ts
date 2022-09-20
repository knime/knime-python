import { createApp } from 'vue';
import consola from 'consola';
import { configureCompat } from 'vue';

import App from './App.vue';

(window as any).consola = consola.create({ // eslint-disable-line
    level: 4 // TODO: make configurable
});


configureCompat({ RENDER_FUNCTION: false });

const app = createApp(App);

app.mount('#app');
