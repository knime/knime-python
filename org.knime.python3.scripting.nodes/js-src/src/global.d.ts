import { Consola } from "consola";

declare global {
  interface Window {
    consola: Consola;
  }
}
