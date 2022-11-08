const iFrameKnimeService = new KnimeUIExtensionService.IFrameKnimeService();
const selectionService = new KnimeUIExtensionService.SelectionService(iFrameKnimeService);

const d = document.getElementById('{plot_id}');
let selected = []


/*
 * TODO get the initial selection from selectionService.initialSelection
 */

/*
 * TODO `selected` should probably be a Set that is updated correctly
 * if selections in other views are added.
 */

/*
 * TODO for facelets the selected data is only the data of the current
 * facelet. However, the selection in the other facelets does not change.
 * Therefore, calling replace is wong in this case.
 */

d.on('plotly_selected', function (data) {
    if (data === undefined) {
        // Deselection is handled by on plotly_deselect
        return;
    }

    selected = data.points.map(p => p.customdata[0]);
    selectionService.replace(selected);
});

d.on('plotly_deselect', function () {
    // Make sure we deselect everything: All facelets
    Plotly.restyle(d, { selectedpoints: [null] });
    selectionService.replace([]);
});

selectionService.addOnSelectionChangeCallback(event => {
    // Update the selected keys
    if (event.mode == "REPLACE") {
        selected = event.selection;
    } else if (event.mode == "ADD") {
        selected.push(...event.selection);
    } else if (event.mode == "REMOVE") {
        selected = selected.filter(item => !event.selection.includes(item))
    }

    if (selected.length == 0) {
        // Reset selection for all traces
        Plotly.restyle(d, { selectedpoints: [null] });
    } else {
        // Loop over traces
        for (let i = 0; i < d.data.length; i++) {

            // Find the indices of the selected points for this trace
            let indices = []
            for (let j = 0; j < d.data[i].customdata.length; j++) {
                if (selected.includes(d.data[i].customdata[j][0])) {
                    indices.push(j);
                }
            }

            // Update this trace
            Plotly.restyle(d, { selectedpoints: [indices] }, [i]);
        }
    }
});