import PythonEditorControls from "@/PythonEditorControls.vue";
import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";

describe("PythonEditorControls", () => {
  const doMount = ({ props } = { props: {} }) => {
    const wrapper = mount(PythonEditorControls, { props });
    return { wrapper };
  };

  it("renders controls", () => {
    const { wrapper } = doMount();
    expect(wrapper.find(".left-button").exists()).toBeTruthy();
    expect(wrapper.find(".right-button").exists()).toBeTruthy();
  });
});
