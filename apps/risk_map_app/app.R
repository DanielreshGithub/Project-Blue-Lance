library(shiny)
library(bslib)
library(dplyr)
library(leaflet)
library(htmltools)
library(arrow)

# ===== SETUP =====
APP_DIR <- normalizePath(getwd())
PROJECT_ROOT <- normalizePath(file.path(APP_DIR, "..", ".."))

DATA_MAIN <- file.path(PROJECT_ROOT, "reports", "acled_gdelt_admin1_time_series_8w.parquet")
DATA_DEMO <- file.path(PROJECT_ROOT, "reports", "demo_acled_gdelt_admin1_time_series_8w.parquet")
CENTROIDS_PATH <- file.path(PROJECT_ROOT, "data", "processed", "acled_global_weekly_features.csv")

pick_data_path <- function() {
  if (file.exists(DATA_MAIN) && file.info(DATA_MAIN)$size > 0) return(DATA_MAIN)
  if (file.exists(DATA_DEMO) && file.info(DATA_DEMO)$size > 0) return(DATA_DEMO)
  return(NA_character_)
}

# ===== UI =====
ui <- page_fillable(
  theme = bs_theme(version = 5, bg = "#0a0f1a", fg = "#e4e4e7", primary = "#3b82f6"),

  tags$head(tags$style(HTML("
    body { background: #0a0f1a; font-family: 'Inter', sans-serif; overflow: hidden; }
    #map { width: 100vw; height: 100vh; position: fixed; top: 0; left: 0; }
    .leaflet-container { background: #0a0f1a; max-width: 100vw; max-height: 100vh; }
    .top-bar {
      position: fixed; top: 0; left: 0; right: 0; height: 56px;
      background: rgba(10, 15, 26, 0.9); backdrop-filter: blur(20px);
      border-bottom: 1px solid rgba(255,255,255,0.1);
      z-index: 1000; display: flex; align-items: center;
      padding: 0 20px; gap: 16px;
    }
    .app-title { font-size: 18px; font-weight: 700; color: #fff; }
    .side-panel {
      position: fixed; top: 68px; right: 20px; width: 320px;
      max-height: calc(100vh - 88px);
      background: rgba(10, 15, 26, 0.9); backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.1); border-radius: 12px;
      padding: 16px; z-index: 999; overflow-y: auto;
    }
    .stats { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 16px; }
    .stat { background: rgba(255,255,255,0.05); padding: 8px; border-radius: 8px; }
    .stat-label { font-size: 11px; color: #999; }
    .stat-value { font-size: 16px; font-weight: 600; color: #fff; }
    .risk-item {
      background: rgba(255,255,255,0.05); padding: 10px; border-radius: 8px;
      margin-bottom: 8px; cursor: pointer; transition: 0.2s;
    }
    .risk-item:hover { background: rgba(255,255,255,0.1); }
    .risk-location { font-weight: 600; color: #fff; margin-bottom: 4px; }
    .risk-badge {
      display: inline-block; padding: 2px 8px; border-radius: 4px;
      font-size: 10px; font-weight: 600; text-transform: uppercase;
    }
    .risk-badge.high { background: rgba(239,68,68,0.2); color: #ef4444; }
    .risk-badge.medium { background: rgba(245,158,11,0.2); color: #f59e0b; }
    .risk-badge.low { background: rgba(59,130,246,0.2); color: #3b82f6; }
    .legend {
      position: fixed; left: 20px; bottom: 20px;
      background: rgba(10, 15, 26, 0.9); backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.1); border-radius: 10px;
      padding: 12px; z-index: 999;
    }
    .legend-title { font-size: 11px; font-weight: 600; color: #999; margin-bottom: 8px; }
    .legend-items { display: flex; gap: 12px; }
    .legend-item { display: flex; align-items: center; gap: 6px; }
    .legend-dot { width: 10px; height: 10px; border-radius: 50%; }
    .legend-label { font-size: 12px; color: #ccc; }
  "))),  

  div(class = "top-bar",
      div(class = "app-title", "⚡ Blue Lance Risk Map"),
      textInput("search", NULL, "", placeholder = "Search location...", width = "200px"),
      checkboxInput("only_high", "Show only HIGH risk", FALSE)
  ),

  leafletOutput("map", width = "100%", height = "100%"),

  div(class = "side-panel",
      div(class = "stats",
          div(class = "stat",
              div(class = "stat-label", "WEEK"),
              div(class = "stat-value", textOutput("stat_week", inline = TRUE))
          ),
          div(class = "stat",
              div(class = "stat-label", "REGIONS"),
              div(class = "stat-value", textOutput("stat_regions", inline = TRUE))
          ),
          div(class = "stat",
              div(class = "stat-label", "HIGH RISK"),
              div(class = "stat-value", textOutput("stat_high", inline = TRUE))
          ),
          div(class = "stat",
              div(class = "stat-label", "TOTAL RISK"),
              div(class = "stat-value", textOutput("stat_total", inline = TRUE))
          )
      ),

      sliderInput("week", "Select Week", min = 1, max = 8, value = 8, width = "100%"),

      tags$div(style = "font-size: 12px; font-weight: 600; color: #999; margin: 16px 0 8px;", "TOP RISKS"),
      uiOutput("risk_list")
  ),

  div(class = "legend",
      div(class = "legend-title", "RISK SEVERITY"),
      div(class = "legend-items",
          div(class = "legend-item",
              div(class = "legend-dot", style = "background: #52525b;"),
              span(class = "legend-label", "None")
          ),
          div(class = "legend-item",
              div(class = "legend-dot", style = "background: #3b82f6;"),
              span(class = "legend-label", "Low")
          ),
          div(class = "legend-item",
              div(class = "legend-dot", style = "background: #f59e0b;"),
              span(class = "legend-label", "Medium")
          ),
          div(class = "legend-item",
              div(class = "legend-dot", style = "background: #ef4444;"),
              span(class = "legend-label", "High")
          )
      )
  )
)

# ===== SERVER =====
server <- function(input, output, session) {

  # ---- Load data (main -> demo fallback) ----
  data_path <- pick_data_path()
  if (is.na(data_path)) {
    message("❌ No dataset found.")
    message("Expected one of:")
    message(" - ", DATA_MAIN)
    message(" - ", DATA_DEMO)
    showNotification(
      "No dataset found. Create demo data (reports/demo_...) or run your pipeline to generate reports/*.parquet",
      type = "error", duration = NULL
    )
    df <- data.frame()
  } else {
    message("Loading data from: ", data_path)
    df <- as.data.frame(arrow::read_parquet(data_path))
  }

  # If no data, still render base map and safe outputs
  if (nrow(df) == 0) {
    weeks <- as.Date(character(0))
  } else {
    # ---- Clean core columns ----
    df$week <- as.Date(df$week)
    df$country <- trimws(toupper(df$country))
    df$admin1 <- trimws(toupper(df$admin1))

    # Severity
    if ("severity_label_next_week" %in% names(df)) {
      df$severity <- tolower(trimws(df$severity_label_next_week))
    } else {
      df$severity <- "none"
    }

    # ---- Coordinates: use parquet lat/lon if available, else merge centroids ----
    has_latlon <- ("lat" %in% names(df)) && ("lon" %in% names(df)) &&
      any(!is.na(df$lat)) && any(!is.na(df$lon))

    if (!has_latlon && file.exists(CENTROIDS_PATH)) {
      message("Lat/lon missing -> loading centroids: ", CENTROIDS_PATH)
      cent <- read.csv(CENTROIDS_PATH, stringsAsFactors = FALSE)

      cent$country <- trimws(toupper(cent$country))
      cent$admin1 <- trimws(toupper(cent$admin1))
      cent$lat <- as.numeric(cent$centroid_latitude)
      cent$lon <- as.numeric(cent$centroid_longitude)

      cent <- cent[!is.na(cent$lat) & !is.na(cent$lon), ]
      cent <- cent[!duplicated(paste(cent$country, cent$admin1)), ]
      cent <- cent[c("country", "admin1", "lat", "lon")]

      df <- merge(df, cent, by = c("country", "admin1"), all.x = TRUE)
    }

    # Clean coords + drop missing
    if ("lat" %in% names(df)) df$lat <- as.numeric(df$lat)
    if ("lon" %in% names(df)) df$lon <- as.numeric(df$lon)
    df <- df[!is.na(df$lat) & !is.na(df$lon), ]

    message("Data loaded: ", nrow(df), " rows with coordinates")

    weeks <- sort(unique(df$week))
  }

  observe({
    if (length(weeks) > 0) {
      updateSliderInput(session, "week", min = 1, max = length(weeks), value = length(weeks))
    } else {
      updateSliderInput(session, "week", min = 1, max = 1, value = 1)
    }
  })

  # Current data
  current_data <- reactive({
    if (length(weeks) == 0 || nrow(df) == 0) return(df[0, ])
    selected_week <- weeks[input$week]
    data <- df[df$week == selected_week, ]

    if (nzchar(trimws(input$search))) {
      search_term <- toupper(trimws(input$search))
      data <- data[grepl(search_term, data$country) | grepl(search_term, data$admin1), ]
    }

    if (isTRUE(input$only_high)) {
      data <- data[data$severity == "high", ]
    }

    data
  })

  # Stats
  output$stat_week <- renderText({
    if (length(weeks) == 0) return("—")
    format(weeks[input$week], "%b %d")
  })
  output$stat_regions <- renderText({ nrow(current_data()) })
  output$stat_high <- renderText({ sum(current_data()$severity == "high", na.rm = TRUE) })
  output$stat_total <- renderText({ sum(current_data()$severity != "none", na.rm = TRUE) })

  # Map
  output$map <- renderLeaflet({
    leaflet(options = leafletOptions(
      minZoom = 2,
      maxZoom = 12,
      worldCopyJump = FALSE,
      maxBounds = list(list(-90, -180), list(90, 180)),
      maxBoundsViscosity = 1.0
    )) %>%
      addProviderTiles(providers$CartoDB.DarkMatter) %>%
      setView(lng = 20, lat = 20, zoom = 2) %>%
      setMaxBounds(lng1 = -180, lat1 = -90, lng2 = 180, lat2 = 90)
  })

  # Update map markers
  observe({
    data <- current_data()
    leafletProxy("map") %>% clearMarkers()
    if (nrow(data) == 0) return()

    colors <- ifelse(data$severity == "high", "#ef4444",
              ifelse(data$severity == "medium", "#f59e0b",
              ifelse(data$severity == "low", "#3b82f6", "#52525b")))

    sizes <- ifelse(data$severity == "high", 10,
             ifelse(data$severity == "medium", 8,
             ifelse(data$severity == "low", 6, 4)))

    popups <- paste0(
      "<b>", data$country, "</b><br>",
      data$admin1, "<br>",
      "<b>Risk:</b> ", toupper(data$severity)
    )

    leafletProxy("map") %>%
      addCircleMarkers(
        lng = data$lon,
        lat = data$lat,
        radius = sizes,
        color = colors,
        fillColor = colors,
        fillOpacity = 0.7,
        stroke = FALSE,
        popup = popups,
        layerId = paste(data$country, data$admin1, sep = "|")
      )
  })

  # Risk list
  output$risk_list <- renderUI({
    data <- current_data()
    if (nrow(data) == 0) {
      return(div(style = "color: #999; font-size: 13px;", "No regions found"))
    }

    rank <- c("none" = 0, "low" = 1, "medium" = 2, "high" = 3)
    data$rank <- rank[data$severity]
    data <- data[order(-data$rank), ]
    top_10 <- head(data, 10)

    items <- lapply(seq_len(nrow(top_10)), function(i) {
      row <- top_10[i, ]
      severity <- row$severity

      badge_class <- ""
      if (severity == "high") badge_class <- "high"
      else if (severity == "medium") badge_class <- "medium"
      else if (severity == "low") badge_class <- "low"

      div(
        class = "risk-item",
        onclick = sprintf(
          "Shiny.setInputValue('zoom_to', '%s|%s', {priority: 'event'})",
          row$country, row$admin1
        ),
        div(class = "risk-location", paste0(row$country, " — ", row$admin1)),
        if (badge_class != "") span(class = paste("risk-badge", badge_class), toupper(severity))
      )
    })

    tagList(items)
  })

  # Zoom to location
  observeEvent(input$zoom_to, {
    parts <- strsplit(input$zoom_to, "\\|")[[1]]
    if (length(parts) != 2) return()

    country <- parts[1]
    admin1 <- parts[2]

    data <- current_data()
    row <- data[data$country == country & data$admin1 == admin1, ]

    if (nrow(row) > 0) {
      leafletProxy("map") %>%
        setView(lng = row$lon[1], lat = row$lat[1], zoom = 6)
    }
  })
}

shinyApp(ui, server)