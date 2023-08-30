import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import PythonWorkspace from "../PythonWorkspace.vue";
import { getScriptingService } from "@knime/scripting-editor";

describe("PythonWorkspace", () => {
  const doMount = ({ props } = { props: {} }) => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
      return Promise.resolve({
        status: "SUCCESS",
        description: "mocked execution info",
      });
    });
    const wrapper = mount(PythonWorkspace, { props });
    return { wrapper };
  };

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllTimers();
  });

  it("renders restart button", () => {
    const { wrapper } = doMount();
    expect(wrapper.find(".restart-button").exists()).toBeTruthy();
  });

  it("restarts python session when restart button is clicked", async () => {
    const sendToServiceSpy = vi.spyOn(getScriptingService(), "sendToService");
    const { wrapper } = doMount();
    await wrapper.find(".restart-button").trigger("click");
    vi.runAllTimers();
    await flushPromises();
    expect(sendToServiceSpy).toHaveBeenNthCalledWith(1, "killSession");
    expect(sendToServiceSpy).toHaveBeenNthCalledWith(
      2,
      "startInteractive",
      expect.anything(),
    );
  });
});
