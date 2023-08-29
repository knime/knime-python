import mockScriptingService from "@/__mocks__/scripting-service";
import PythonEditorControls from "../PythonEditorControls.vue";
import { DOMWrapper, VueWrapper, flushPromises, mount } from "@vue/test-utils";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
  type SpyInstance,
} from "vitest";
import PlayIcon from "webapps-common/ui/assets/img/icons/play.svg";
import LoadingIcon from "webapps-common/ui/components/LoadingIcon.vue";
import CancelIcon from "webapps-common/ui/assets/img/icons/circle-close.svg";

describe("PythonEditorControls", () => {
  const doMount = ({ props } = { props: {} }) => {
    const wrapper = mount(PythonEditorControls, { props });
    return { wrapper };
  };

  let sendToServiceSpy: SpyInstance;

  beforeEach(() => {
    vi.useFakeTimers();
    sendToServiceSpy = vi.spyOn(mockScriptingService, "sendToService");
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllTimers();
  });

  it("renders controls", () => {
    const { wrapper } = doMount();
    expect(wrapper.find(".run-all-button").exists()).toBeTruthy();
    expect(wrapper.find(".run-selected-button").exists()).toBeTruthy();
  });

  it("starts python session on mount", () => {
    doMount();
    expect(sendToServiceSpy).toHaveBeenCalledWith(
      "startInteractive",
      expect.anything(),
    );
  });

  describe("script execution", () => {
    let wrapper: VueWrapper;

    beforeEach(() => {
      wrapper = doMount().wrapper;
      expect(sendToServiceSpy).toHaveBeenNthCalledWith(
        1,
        "startInteractive",
        expect.anything(),
      );
    });

    it("starts new python process and executes script when clicking on run all button", async () => {
      const runAllButton = wrapper.find(".run-all-button");
      await runAllButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(sendToServiceSpy).toHaveBeenNthCalledWith(
        2,
        "startInteractive",
        expect.anything(),
      );
      expect(sendToServiceSpy).toHaveBeenNthCalledWith(3, "runInteractive", [
        "myScript",
        false,
      ]);
    });

    it("executes selected lines in current python session when clicking on run selected lines button", async () => {
      const runSelectedButton = wrapper.find(".run-selected-button");
      await runSelectedButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(sendToServiceSpy).toHaveBeenNthCalledWith(2, "runInteractive", [
        "mySelectedLines",
        false,
      ]);
    });

    it("cancels script execution when clicking button during execution", async () => {
      const runSelectedButton = wrapper.find(".run-selected-button");
      await runSelectedButton.trigger("click");
      await runSelectedButton.trigger("click");
      vi.runAllTimers();
      await flushPromises();
      expect(sendToServiceSpy).toHaveBeenNthCalledWith(2, "runInteractive", [
        "mySelectedLines",
        false,
      ]);
      expect(sendToServiceSpy).toHaveBeenNthCalledWith(3, "killSession");
      expect(sendToServiceSpy).toHaveBeenNthCalledWith(
        4,
        "startInteractive",
        expect.anything(),
      );
    });
  });

  describe("button texts", () => {
    let wrapper: VueWrapper,
      runAllButton: DOMWrapper<Element>,
      runSelectedButton: DOMWrapper<Element>;

    beforeEach(() => {
      wrapper = doMount().wrapper;
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
      await runAllButton.trigger("click");
      await runAllButton.trigger("mouseover");
      expect(runAllButton.text()).toBe("Cancel");
      expect(runAllButton.findComponent(CancelIcon).exists()).toBeTruthy();
      await runAllButton.trigger("mouseleave");
      expect(runAllButton.text()).toContain("Running ...");
    });

    it("displays correct button texts on run selected button mouseover", async () => {
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
});
