import Vue from 'vue';
import App from './App.vue';
import c from 'consola';

// eslint-disable-next-line @typescript-eslint/no-explicit-any, no-extra-parens
(window as any).consola = c.create({
    level: -1
});

Vue.config.productionTip = false;

new Vue({
    render: (h) => h(App)
}).$mount('#app');
