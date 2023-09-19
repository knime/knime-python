import { flushPromises, mount } from "@vue/test-utils";
import { describe, expect, it, vi } from "vitest";
import PythonViewPreview from "../PythonViewPreview.vue";
import { useWorkspaceStore } from "@/store";

describe("PythonViewPreview", () => {
  it("renders iframe", () => {
    const wrapper = mount(PythonViewPreview);
    const iframe = wrapper.find("iframe");
    expect(iframe.exists()).toBeTruthy();
    expect(iframe.attributes("sandbox")).toBe("allow-scripts");
    expect(iframe.attributes("src")).toBe("./preview.html");
  });

  it("reloads iframe when workspace changes", async () => {
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

    // Update the workspace value, simulating a change
    const store = useWorkspaceStore();
    store.workspace = [];

    // Wait for Vue's reactivity system to process changes
    await flushPromises();

    // Check if the iframe's reload function was called
    expect(mockIframeContentWindow.location.reload).toHaveBeenCalled();

    // Delete the mocked contentWindow property
    // @ts-ignore - we set the contentWindow property with configurable: true
    delete window.HTMLIFrameElement.prototype.contentWindow;
  });
});
