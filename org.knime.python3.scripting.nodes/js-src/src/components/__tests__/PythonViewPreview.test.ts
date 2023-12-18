import { mount } from "@vue/test-utils";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import PythonViewPreview from "../PythonViewPreview.vue";
import { usePythonPreviewStatusStore, useSessionStatusStore } from "@/store";
import { editor, getScriptingService } from "@knime/scripting-editor";
import Button from "webapps-common/ui/components/Button.vue";
import { ref } from "vue";

describe("PythonViewPreview", () => {
  const previewStatusStore = usePythonPreviewStatusStore();
  const sendToServiceMock = vi.fn();

  beforeAll(() => {
    getScriptingService().sendToService = sendToServiceMock;
  });

  beforeEach(() => {
    previewStatusStore.hasValidView = false;
    previewStatusStore.isExecutedOnce = false;
    delete previewStatusStore.updateViewCallback;
  });

  it("renders iframe", () => {
    const wrapper = mount(PythonViewPreview);
    const iframe = wrapper.find("iframe");
    expect(iframe.exists()).toBeTruthy();
    expect(iframe.attributes("sandbox")).toBe("allow-scripts");
    expect(iframe.attributes("src")).toBe("./preview.html");
  });

  it("adds update view callback on mount", () => {
    mount(PythonViewPreview);
    expect(previewStatusStore.updateViewCallback).toBeDefined();
  });

  it("reloads iframe when update view callback is called", () => {
    // Mock the iframe's contentWindow
    const mockIframeContentWindow = {
      location: {
        reload: vi.fn(),
      },
    };

    // Use Object.defineProperty to mock the iframe's contentWindow
    Object.defineProperty(window.HTMLIFrameElement.prototype, "contentWindow", {
      get: () => mockIframeContentWindow,
      configurable: true,
    });

    mount(PythonViewPreview);

    // mock updating the view via the provided callback
    const store = usePythonPreviewStatusStore();
    store.updateViewCallback!();

    expect(mockIframeContentWindow.location.reload).toHaveBeenCalled();

    delete store.updateViewCallback;
  });

  it("shows view if valid view exists", () => {
    previewStatusStore.hasValidView = true;
    const wrapper = mount(PythonViewPreview);
    expect(wrapper.find(".iframe-container").isVisible()).toBeTruthy();
    expect(wrapper.find(".placeholder-container").isVisible()).toBeFalsy();
  });

  describe("placeholder", () => {
    it("shows placeholder if valid view does not exist", () => {
      const wrapper = mount(PythonViewPreview);
      expect(wrapper.find(".iframe-container").isVisible()).toBeFalsy();
      expect(wrapper.find(".placeholder-container").isVisible()).toBeTruthy();
      expect(wrapper.find("#preview-img").isVisible()).toBeTruthy();
    });

    it("shows placeholder text before first execution", () => {
      const wrapper = mount(PythonViewPreview);
      expect(wrapper.find(".iframe-container").isVisible()).toBeFalsy();
      expect(wrapper.find(".placeholder-container").isVisible()).toBeTruthy();
      expect(wrapper.find(".placeholder-text").text()).toContain(
        "Please run the code to see the preview.",
      );
    });

    it("shows error text after first execution", () => {
      previewStatusStore.isExecutedOnce = true;
      const wrapper = mount(PythonViewPreview);
      expect(wrapper.find(".iframe-container").isVisible()).toBeFalsy();
      expect(wrapper.find(".placeholder-container").isVisible()).toBeTruthy();
      expect(wrapper.find(".placeholder-text").text()).toContain(
        "The view cannot be displayed.",
      );
    });

    it("placholder button executes script", () => {
      editor.useMainCodeEditorStore().value = { text: ref("myScript") } as any;
      previewStatusStore.hasValidView = false;
      const wrapper = mount(PythonViewPreview);
      wrapper.find("button").trigger("click");
      expect(sendToServiceMock).toHaveBeenCalledWith("runScript", ["myScript"]);
    });

    it("shows placeholder button", () => {
      useSessionStatusStore().status = "IDLE";
      useSessionStatusStore().isRunningSupported = true;
      const wrapper = mount(PythonViewPreview);
      const button = wrapper.findComponent(Button);
      expect(button.exists()).toBeTruthy();
      expect(button.props().disabled).toBeFalsy();
    });

    it("disables placeholder button if no inputs available", () => {
      useSessionStatusStore().status = "IDLE";
      useSessionStatusStore().isRunningSupported = false;
      const wrapper = mount(PythonViewPreview);
      const button = wrapper.findComponent(Button);
      expect(button.props().disabled).toBeTruthy();
    });

    it("disables placeholder button if running all", () => {
      useSessionStatusStore().status = "RUNNING_ALL";
      useSessionStatusStore().isRunningSupported = true;
      const wrapper = mount(PythonViewPreview);
      const button = wrapper.findComponent(Button);
      expect(button.props().disabled).toBeTruthy();
    });

    it("disables placeholder button if running selected lines", () => {
      useSessionStatusStore().status = "RUNNING_SELECTED";
      useSessionStatusStore().isRunningSupported = true;
      const wrapper = mount(PythonViewPreview);
      const button = wrapper.findComponent(Button);
      expect(button.props().disabled).toBeTruthy();
    });
  });
});
