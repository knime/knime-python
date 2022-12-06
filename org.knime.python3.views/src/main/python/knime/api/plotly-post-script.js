const iFrameKnimeService = new KnimeUIExtensionService.IFrameKnimeService();
const selectionService = new KnimeUIExtensionService.SelectionService(
  iFrameKnimeService
);

const plotlyPlot = document.getElementById("{plot_id}");
let selected = new Set();

///////////////////////////////////////
///////////////////////////////////////
// TODO WRITE DOWN THE LIMITATIONS SOMEWHERE
///////////////////////////////////////
///////////////////////////////////////

/*
 * TODO BUG
 * 1. Select some points in a plotly plot
 * 2. Select some points in another plot
 *    -> The plotly selection updates
 * 3. Do a "shift" selection on the plotly plot
 *    -> The selection from step 1 becomes visible again and the
 *       selection from step 2 is deleted
 */

/////////////////////////////////////////////////////////////////////
// KNIME Selection -> Plotly Selection
/////////////////////////////////////////////////////////////////////

function updateSelection(mode, selection) {
  // Update the selected keys
  if (mode == "REPLACE") {
    selected = new Set(selection);
  } else if (mode == "ADD") {
    selection.forEach((s) => selected.add(s));
  } else if (mode == "REMOVE") {
    selection.forEach((s) => selected.delete(s));
  }

  // Restyle the plot with the current selection
  if (selected.size == 0) {
    // Reset selection for all traces
    Plotly.restyle(plotlyPlot, { selectedpoints: [null] });
  } else {
    // Loop over traces
    for (let i = 0; i < plotlyPlot.data.length; i++) {
      // Find the indices of the selected points for this trace
      let indices = [];
      for (let j = 0; j < plotlyPlot.data[i].customdata.length; j++) {
        if (selected.has(plotlyPlot.data[i].customdata[j][0])) {
          indices.push(j);
        }
      }

      // Update this trace
      Plotly.restyle(plotlyPlot, { selectedpoints: [indices] }, [i]);
    }
  }
}

iFrameKnimeService.waitForInitialization().then(() => {
  // Register the selection listener
  selectionService.onInit(
    (event) => updateSelection(event.mode, event.selection),
    true,
    true
  );

  // NOTE: selectionService.initialSelection does not work because the implementation
  // only returns an inital selection if initalData is not undefined

  // Get the inital selection and apply it
  const initialSelection =
    selectionService.knimeService.extensionConfig.initialSelection;
  updateSelection("REPLACE", initialSelection);
});

/////////////////////////////////////////////////////////////////////
// Plotly Selection -> KNIME Selection
/////////////////////////////////////////////////////////////////////

plotlyPlot.on("plotly_selected", function () {
  // Deselect all points on traces without any selection
  plotlyPlot.data.forEach((d, idx) => {
    if (d.selectedpoints === undefined) {
      Plotly.restyle(plotlyPlot, { selectedpoints: [[]] }, [idx]);
    }
  });

  // Get the customdata for the selected points of every trace
  const selectedRows = plotlyPlot.data.flatMap((d) => {
    return d.selectedpoints.map((i) => d.customdata[i][0]);
  });

  // Send the selection to the SelectionService
  selectionService.replace(selectedRows);

  // Remember the selection to be able to add points to it
  selected = new Set(selectedRows);
});

plotlyPlot.on("plotly_deselect", function () {
  // Make sure we deselect everything: All facelets
  Plotly.restyle(plotlyPlot, { selectedpoints: [null] });
  selectionService.replace([]);
});

