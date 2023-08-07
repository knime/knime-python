import { describe, it, expect } from "vitest";
import { shallowMount } from "@vue/test-utils";
import App from "../App.vue";

describe("App.vue", () => {
  it("renders the CodeEditor component with the correct language", () => {
    const wrapper = shallowMount(App);
    const helloWorldComponent = wrapper.findComponent({ name: "CodeEditor" });
    expect(helloWorldComponent.props("language")).toBe("python");
  });
});
