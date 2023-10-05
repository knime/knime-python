import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import PythonWorkspace from "../PythonWorkspace.vue";
import { getScriptingService } from "@knime/scripting-editor";
import Button from "webapps-common/ui/components/Button.vue";
import { useWorkspaceStore } from "@/store";
import TableHeader, { type ColumnSizes } from "../TableHeader.vue";
import { type Ref } from "vue";

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
    return { wrapper, resetButton: wrapper.find(".reset-button") };
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

  it("emit resize", async () => {
    const { wrapper } = await doMount();

    const header = wrapper.findComponent(TableHeader);
    expect((wrapper.vm as WorkspaceState).resizeContainer).not.toBeNull();
    (wrapper.vm as WorkspaceState).totalWidth = 510;
    await flushPromises();
    await header.vm.$emit("resize");
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
    vi.mocked(getScriptingService().inputsAvailable).mockImplementation(() => {
      return Promise.resolve(false);
    });

    const { wrapper } = await doMount();
    const button = wrapper.findComponent(Button);
    expect(button.props().disabled).toBeTruthy();
  });
});
