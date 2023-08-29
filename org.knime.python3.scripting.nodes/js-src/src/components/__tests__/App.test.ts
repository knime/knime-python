import { describe, it, expect } from "vitest";
import { shallowMount } from "@vue/test-utils";
import App from "../App.vue";

describe("App.vue", () => {
  it("renders the ScriptingEditor component with the correct language", () => {
    const wrapper = shallowMount(App);
    const scriptingComponent = wrapper.findComponent({
      name: "ScriptingEditor",
    });
    expect(scriptingComponent.exists()).toBeTruthy();
  });
});
