library(readr)
library(dplyr)
library(leaflet)
library(leaflet.extras)
library(htmlwidgets)

pred_path <- "reports/latest_risk_predictions_global.csv"
feat_path <- "data/processed/acled_global_weekly_features.csv"

pred <- read_csv(pred_path, show_col_types = FALSE)
feat <- read_csv(feat_path, show_col_types = FALSE)

pred$week <- as.Date(pred$week)
feat$week <- as.Date(feat$week)

latest_week <- max(pred$week, na.rm = TRUE)
latest <- pred %>% filter(week == latest_week)

# Get one centroid per (country, admin1)
centroids <- feat %>%
  arrange(country, admin1, desc(week)) %>%
  group_by(country, admin1) %>%
  summarise(
    lat = first(centroid_latitude),
    lon = first(centroid_longitude),
    .groups = "drop"
  )

m <- latest %>%
  left_join(centroids, by = c("country","admin1")) %>%
  filter(!is.na(lat), !is.na(lon))

# Ensure labels are consistent
m$predicted_severity_label_next_week <- tolower(m$predicted_severity_label_next_week)

# Colors by band
m$col <- "gray"
m$col[m$predicted_severity_label_next_week == "low"] <- "yellow"
m$col[m$predicted_severity_label_next_week == "medium"] <- "orange"
m$col[m$predicted_severity_label_next_week == "high"] <- "red"

# Radius by confidence (if column exists), else constant
if ("predicted_confidence" %in% names(m)) {
  m$predicted_confidence <- pmax(pmin(m$predicted_confidence, 1), 0)
  m$radius <- 4 + 10 * m$predicted_confidence
} else {
  m$radius <- 7
}

# Popup text
m$popup <- paste0(
  "<b>", m$country, "</b><br>",
  m$admin1, "<br>",
  "Input week: ", m$week, "<br>",
  "<b>Pred next-week risk:</b> ", m$predicted_severity_label_next_week,
  if ("predicted_confidence" %in% names(m)) paste0("<br>Confidence: ", round(m$predicted_confidence, 3)) else ""
)

# Make filtered layers
m_all <- m
m_high <- m %>% filter(predicted_severity_label_next_week == "high")
m_medhigh <- m %>% filter(predicted_severity_label_next_week %in% c("medium","high"))

map <- leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%

  # ALL layer
  addCircleMarkers(
    data = m_all, lng = ~lon, lat = ~lat,
    radius = ~radius, color = ~col,
    stroke = FALSE, fillOpacity = 0.75,
    popup = ~popup, group = "All"
  ) %>%

  # High only
  addCircleMarkers(
    data = m_high, lng = ~lon, lat = ~lat,
    radius = ~radius, color = ~col,
    stroke = FALSE, fillOpacity = 0.85,
    popup = ~popup, group = "High only"
  ) %>%

  # Medium + High
  addCircleMarkers(
    data = m_medhigh, lng = ~lon, lat = ~lat,
    radius = ~radius, color = ~col,
    stroke = FALSE, fillOpacity = 0.80,
    popup = ~popup, group = "Medium + High"
  ) %>%

  addLayersControl(
    overlayGroups = c("All", "Medium + High", "High only"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%

  addLegend(
    position = "bottomright",
    colors = c("gray","yellow","orange","red"),
    labels = c("none","low","medium","high"),
    title = paste0("Predicted risk (next week)\nInput week: ", latest_week)
  ) %>%

  # Search box (search country/admin1)
  addSearchFeatures(
    targetGroups = c("All"),
    options = searchFeaturesOptions(
      zoom = 6, openPopup = TRUE, firstTipSubmit = TRUE, autoCollapse = TRUE, hideMarkerOnCollapse = TRUE
    )
  )

out_html <- "reports/latest_risk_map_fancy.html"
saveWidget(map, out_html, selfcontained = FALSE)

cat("Saved:", out_html, "\n")