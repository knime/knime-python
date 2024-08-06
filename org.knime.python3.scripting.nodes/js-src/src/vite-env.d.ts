/// <reference types="@knime/styles/config/svg.d.ts" />
/// <reference types="vite/client" />
/// <reference types="vite-svg-loader" />
/// <reference types="@knime/utils/globals.d.ts" />

declare module "*.vue" {
  import { DefineComponent } from "vue";
  const component: DefineComponent<{}, {}, any>;
  export default component;
}
