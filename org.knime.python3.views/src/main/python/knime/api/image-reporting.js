KnimeUIExtensionService.ReportingService.getInstance().then((service) => {
  const viewElement = document.getElementById("view-container");

  // If the view element is an SVG, ensure it scales properly
  const svg = viewElement.querySelector("svg");
  if (svg) {
    svg.style.width = "100%";
    svg.style.height = "100%";
    svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
  }

  if (viewElement) {
    service.setReportingContent(viewElement.outerHTML);
  } else {
    service.setReportingContent("<p>View element not found.</p>");
  }
});
