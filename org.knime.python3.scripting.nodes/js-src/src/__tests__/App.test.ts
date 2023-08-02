import { describe, it, expect } from "vitest";
import { shallowMount } from "@vue/test-utils";
import App from "../App.vue";

describe("App.vue", () => {
  it("renders the HelloWorld component with the correct message", () => {
    const wrapper = shallowMount(App);
    const helloWorldComponent = wrapper.findComponent({ name: "HelloWorld" });
    expect(helloWorldComponent.props("msg")).toBe("You did it!");
  });
});
