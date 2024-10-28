import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { type Ref } from "vue";
import { flushPromises, mount } from "@vue/test-utils";

import { Button } from "@knime/components";
import { getScriptingService } from "@knime/scripting-editor";

import { DEFAULT_INITIAL_DATA } from "@/__mocks__/mock-data";
import { getPythonInitialDataService } from "@/python-initial-data-service";
import { useSessionStatusStore, useWorkspaceStore } from "@/store";
import PythonWorkspace from "../PythonWorkspace.vue";
import { type ColumnSizes } from "../PythonWorkspaceHeader.vue";

type WorkspaceState = {
  headerWidths?: ColumnSizes;
  totalWidth?: number;
  resizeContainer?: Ref<HTMLElement>;
};

describe("PythonWorkspace", () => {
  const doMount = async ({ props } = { props: {} }) => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
      return Promise.resolve({
        status: "SUCCESS",
        description: "mocked execution info",
      });
    });
    const wrapper = mount(PythonWorkspace, { props });
    await flushPromises();
    const sessionStatus = useSessionStatusStore();
    return {
      wrapper,
      resetButton: wrapper.find(".reset-button"),
      sessionStatus,
    };
  };

  beforeEach(() => {
    const store = useWorkspaceStore();
    store.workspace = undefined;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllTimers();
  });

  it("renders reset button", async () => {
    const { resetButton } = await doMount();
    expect(resetButton.exists()).toBeTruthy();
  });

  it("should update table header widths on resize", async () => {
    const { wrapper } = await doMount();

    expect((wrapper.vm as WorkspaceState).resizeContainer).not.toBeNull();
    (wrapper.vm as WorkspaceState).totalWidth = 510;
    await flushPromises();
    expect((wrapper.vm as WorkspaceState).headerWidths).toStrictEqual([
      170, 170, 170,
    ]);
  });

  it("reset python session when reset button is clicked", async () => {
    const { resetButton } = await doMount();
    const sendToServiceSpy = vi.spyOn(getScriptingService(), "sendToService");
    await resetButton.trigger("click");
    vi.runAllTimers();
    await flushPromises();
    expect(sendToServiceSpy).toHaveBeenNthCalledWith(1, "killSession");
  });

  describe("updates session status when reset values is clicked", () => {
    beforeEach(() => {
      const sessionStatus = useSessionStatusStore();
      sessionStatus.status = "IDLE";
      delete sessionStatus.lastActionResult;
    });

    it("on success", async () => {
      const { resetButton, sessionStatus } = await doMount();
      getScriptingService().sendToService = vi.fn(() => {
        return Promise.resolve({ status: "SUCCESS", description: "" });
      });
      await resetButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(sessionStatus.lastActionResult).toBe("RESET");
    });

    it("on failure", async () => {
      const { resetButton, sessionStatus } = await doMount();
      getScriptingService().sendToService = vi.fn(() => {
        return Promise.resolve({ status: "FAILURE", description: "" });
      });
      await resetButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(sessionStatus.lastActionResult).toBe("RESET_FAILED");
    });
  });

  it("button click should reset store", async () => {
    const { resetButton } = await doMount();
    const store = useWorkspaceStore();
    expect(store.workspace).toBeUndefined();
    store.workspace = [{ name: "test", value: "test", type: "test" }];
    expect(store.workspace).toBeDefined();
    resetButton.trigger("click");
    await flushPromises();
    expect(store.workspace).toStrictEqual([]);
  });

  it("reset button enabled if inputs are available", async () => {
    const { wrapper } = await doMount();
    const button = wrapper.findComponent(Button);
    expect(button.props().disabled).toBeFalsy();
  });

  it("reset button disabled if inputs are not available", async () => {
    vi.mocked(getPythonInitialDataService).mockReturnValue({
      getInitialData: () =>
        Promise.resolve({
          ...DEFAULT_INITIAL_DATA,
          inputConnectionInfo: [
            { status: "OK", isOptional: false },
            { status: "UNEXECUTED_CONNECTION", isOptional: false },
          ],
        }),
    });

    const { wrapper } = await doMount();
    const button = wrapper.findComponent(Button);
    expect(button.props().disabled).toBeTruthy();
  });
});
