const selectionService = KnimeUIExtensionService.SelectionService.getInstance();

const plotlyPlot = document.getElementById("{plot_id}");
let selected = new Set();
let updating = false;

/////////////////////////////////////////////////////////////////////
// KNIME Selection -> Plotly Selection
/////////////////////////////////////////////////////////////////////

function updateSelection(mode, selection) {
  // Deactivate plotly_selected events
  updating = true;

  // Delete selection boxes and lassos
  Plotly.relayout(plotlyPlot, { selections: [] });

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

  // Re-activate plotly_selected events
  updating = false;
}

// Register the selection listener
selectionService.then((service) => {
  service.addOnSelectionChangeCallback((event) =>
    updateSelection(event.mode, event.selection)
  );
});

// NOTE: selectionService.initialSelection does not work because the implementation
// only returns an inital selection if initalData is not undefined

// Get the inital selection and apply it
selectionService
  .then((service) => service.baseService.getConfig().initialSelection)
  .then((selection) => updateSelection("REPLACE", selection));

/////////////////////////////////////////////////////////////////////
// Plotly Selection -> KNIME Selection
/////////////////////////////////////////////////////////////////////

plotlyPlot.on("plotly_selected", function () {
  if (updating) {
    // Do not do anything if we are updating the plot because of a selection event by KNIME
    return;
  }

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
  selectionService.then((service) => service.replace(selectedRows));

  // Remember the selection to be able to add points to it
  selected = new Set(selectedRows);
});

plotlyPlot.on("plotly_deselect", function () {
  // Make sure we deselect everything: All facelets
  Plotly.relayout(plotlyPlot, { selections: [] });
  Plotly.restyle(plotlyPlot, { selectedpoints: [null] });
  selectionService.then((service) => service.replace([]));
});
