import { useSessionStatusStore } from "@/store";
import { getScriptingService } from "@knime/scripting-editor";
import { DOMWrapper, VueWrapper, flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import CancelIcon from "webapps-common/ui/assets/img/icons/circle-close.svg";
import PlayIcon from "webapps-common/ui/assets/img/icons/play.svg";
import Button from "webapps-common/ui/components/Button.vue";
import LoadingIcon from "webapps-common/ui/components/LoadingIcon.vue";
import PythonEditorControls from "../PythonEditorControls.vue";

describe("PythonEditorControls", () => {
  const doMount = async ({ props } = { props: {} }) => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
      return Promise.resolve({
        status: "SUCCESS",
        description: "mocked execution info",
      });
    });
    const wrapper = mount(PythonEditorControls, { props });
    await flushPromises(); // to wait for PythonEditorControls.onMounted to finish
    return { wrapper };
  };

  beforeEach(() => {
    vi.useFakeTimers();
    useSessionStatusStore().status = "IDLE";
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllTimers();
  });

  it("renders controls", async () => {
    const { wrapper } = await doMount();
    expect(wrapper.find(".run-all-button").exists()).toBeTruthy();
    expect(wrapper.find(".run-selected-button").exists()).toBeTruthy();
  });

  describe("script execution", () => {
    let wrapper: VueWrapper;

    beforeEach(async () => {
      wrapper = (await doMount()).wrapper;
    });

    it("starts new python process and executes script when clicking on run all button", async () => {
      const runAllButton = wrapper.find(".run-all-button");
      await runAllButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
        1,
        "runScript",
        ["myScript"],
      );
      expect(useSessionStatusStore().status).toBe("RUNNING");
    });

    it("executes selected lines in current python session when clicking on run selected lines button", async () => {
      const runSelectedButton = wrapper.find(".run-selected-button");
      await runSelectedButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
        1,
        "runInExistingSession",
        ["mySelectedLines"],
      );
      expect(useSessionStatusStore().status).toBe("RUNNING");
    });

    it("cancels script execution when clicking button during execution", async () => {
      const runSelectedButton = wrapper.find(".run-selected-button");
      await runSelectedButton.trigger("click");
      await runSelectedButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
        1,
        "runInExistingSession",
        ["mySelectedLines"],
      );
      expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
        2,
        "killSession",
      );
    });
  });

  describe("button texts", () => {
    let wrapper: VueWrapper,
      runAllButton: DOMWrapper<Element>,
      runSelectedButton: DOMWrapper<Element>;

    beforeEach(async () => {
      wrapper = (await doMount()).wrapper;
      runAllButton = wrapper.find(".run-all-button");
      runSelectedButton = wrapper.find(".run-selected-button");
    });

    it("displays correct button texts when no script is running", () => {
      expect(runAllButton.text()).toBe("Run all");
      expect(runAllButton.findComponent(PlayIcon).exists()).toBeTruthy();
      expect(runSelectedButton.text()).toBe("Run selected lines");
      expect(runSelectedButton.findComponent(PlayIcon).exists()).toBeTruthy();
    });

    it("displays correct button texts when run all button is clicked", async () => {
      await runAllButton.trigger("click");
      expect(runAllButton.text()).toContain("Running ...");
      expect(runAllButton.findComponent(LoadingIcon).exists()).toBeTruthy();
      expect(runAllButton.findComponent(PlayIcon).exists()).toBeFalsy();
      expect(runSelectedButton.text()).toBe("Run selected lines");
    });

    it("displays correct button texts when run selected lines button is clicked", async () => {
      await runSelectedButton.trigger("click");
      expect(runSelectedButton.text()).toContain("Running ...");
      expect(
        runSelectedButton.findComponent(LoadingIcon).exists(),
      ).toBeTruthy();
      expect(runSelectedButton.findComponent(PlayIcon).exists()).toBeFalsy();
      expect(runAllButton.text()).toBe("Run all");
    });

    it("displays correct button texts on run all button mouseover", async () => {
      vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
        return new Promise(() => {}); // wait forever
      });
      await runAllButton.trigger("click");
      await runAllButton.trigger("mouseover");
      expect(runAllButton.text()).toBe("Cancel");
      expect(runAllButton.findComponent(CancelIcon).exists()).toBeTruthy();
      await runAllButton.trigger("mouseleave");
      expect(runAllButton.text()).toContain("Running ...");
    });

    it("displays correct button texts on run selected button mouseover", async () => {
      vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
        return new Promise(() => {}); // wait forever
      });
      await runSelectedButton.trigger("click");
      await runSelectedButton.trigger("mouseover");
      expect(runSelectedButton.text()).toBe("Cancel");
      expect(runSelectedButton.findComponent(CancelIcon).exists()).toBeTruthy();
      await runSelectedButton.trigger("mouseleave");
      expect(runSelectedButton.text()).toContain("Running ...");
    });

    it("does not display cancel on mouseover if script is not running", async () => {
      await runAllButton.trigger("mouseover");
      expect(runAllButton.text()).toBe("Run all");
      await runSelectedButton.trigger("mouseover");
      expect(runSelectedButton.text()).toBe("Run selected lines");
    });
  });

  describe("input availability", () => {
    let wrapper: VueWrapper,
      runAllButton: VueWrapper,
      runSelectedButton: VueWrapper;

    const mountAndGetButtons = async () => {
      wrapper = (await doMount()).wrapper;
      runAllButton = wrapper.findAllComponents(Button).at(0) as VueWrapper;
      runSelectedButton = wrapper.findAllComponents(Button).at(1) as VueWrapper;
    };

    it("run buttons are enabled when inputs are available", async () => {
      vi.mocked(getScriptingService().inputsAvailable).mockImplementation(
        () => {
          return Promise.resolve(true);
        },
      );
      await mountAndGetButtons();
      expect(runAllButton.props().disabled).toBeFalsy();
      expect(runSelectedButton.props().disabled).toBeFalsy();
      expect(getScriptingService().sendToConsole).not.toHaveBeenCalled();
    });

    it("run buttons are disabled when inputs are missing", async () => {
      vi.mocked(getScriptingService().inputsAvailable).mockImplementation(
        () => {
          return Promise.resolve(false);
        },
      );
      await mountAndGetButtons();
      expect(runAllButton.props().disabled).toBeTruthy();
      expect(runSelectedButton.props().disabled).toBeTruthy();
    });

    it("warning logged to console if inputs are missing", async () => {
      vi.mocked(getScriptingService().inputsAvailable).mockImplementation(
        () => {
          return Promise.resolve(false);
        },
      );
      await doMount();
      expect(getScriptingService().sendToConsole).toHaveBeenCalledWith({
        warning:
          "Missing input data. Connect all input ports and execute preceding nodes to enable script execution.",
      });
    });
  });
});