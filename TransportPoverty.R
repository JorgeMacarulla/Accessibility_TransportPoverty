# ==========================================================================================
# IOI_POV - Transport Poverty Indicators: A Framework for Measuring Access to Essential Services
# ==========================================================================================

# Load required libraries
library(plyr)         
library(dplyr)        
library(tidyverse)    
library(geosphere)
library(gtfsrouter)
library(gtfstools)
library(ggplot2)
library(chron)
library(sp)
library(sf)
library(doParallel)

# Set working directory
dir_trabajo <- "C:/Users/xx/Downloads/IOI_POV/DATOS_BILBAO"
setwd(dir_trabajo)

# 1. LOAD ORIGIN-DESTINATION TRIP MATRIX FROM MITMA
# Choose any regular weekday dataset (e.g., March 15, 2023)
Matriz_viajes <- read.csv("20230315_Viajes_distritos.csv", encoding = "UTF-8", sep = "|")

# Filter for districts of the Bilbao metropolitan area (see corresponding GIS shapefiles on the MITMA website)
codigos_DIS <- c("4801101", "4801308", "4802001", "4802002", "4802003", "4802004", 
                 "4802005", "4802006", "4802007", "4802008", "4890203", "48904_AM")
codigos_DIS <- as.character(codigos_DIS)

# Keep only trips within the city (excluding intra-district trips)
Matriz_Ciudad <- Matriz_viajes[Matriz_viajes$origen %in% codigos_DIS & Matriz_viajes$destino %in% codigos_DIS, ]
Matriz_Ciudad <- Matriz_Ciudad[Matriz_Ciudad$origen != Matriz_Ciudad$destino, ]

# Load centroids for each district
centroides <- read_csv("centroides_buenos.csv")

# Merge centroid coordinates with trips: origin and destination coordinates
Matriz_Ciudad <- merge(Matriz_Ciudad, centroides[, c("ID", "X", "Y")], by.x = "origen", by.y = "ID", all.x = TRUE)
Matriz_Ciudad <- merge(Matriz_Ciudad, centroides[, c("ID", "X", "Y")], by.x = "destino", by.y = "ID", all.x = TRUE)

# Rename columns for clarity
names(Matriz_Ciudad)[names(Matriz_Ciudad) == "X.y"] <- "origen_X"
names(Matriz_Ciudad)[names(Matriz_Ciudad) == "Y.y"] <- "origen_Y"
names(Matriz_Ciudad)[names(Matriz_Ciudad) == "X.x"] <- "destino_X"
names(Matriz_Ciudad)[names(Matriz_Ciudad) == "Y.x"] <- "destino_Y"

# Save resulting matrix as backup
save(Matriz_Ciudad, file = "Matriz_Ciudad.Rda")

# 2. AGGREGATE TRIPS BY ORIGIN-DESTINATION PAIRS (summing all individuals, sexes, purposes, etc.)
groupColumns <- c("origen", "destino")
dataColumns <- c("viajes", "viajes_km")
Ciudad_agregado <- ddply(Matriz_Ciudad, groupColumns, function(x) colSums(x[dataColumns]))

# Add back coordinates to aggregated data
Ciudad_agregado <- merge(Ciudad_agregado, centroides[, c("ID", "X", "Y")], by.x = "origen", by.y = "ID", all.x = TRUE)
names(Ciudad_agregado)[names(Ciudad_agregado) == "X"] <- "origen_X"
names(Ciudad_agregado)[names(Ciudad_agregado) == "Y"] <- "origen_Y"
Ciudad_agregado <- merge(Ciudad_agregado, centroides[, c("ID", "X", "Y")], by.x = "destino", by.y = "ID", all.x = TRUE)
names(Ciudad_agregado)[names(Ciudad_agregado) == "X"] <- "destino_X"
names(Ciudad_agregado)[names(Ciudad_agregado) == "Y"] <- "destino_Y"

# Save aggregated matrix
save(Ciudad_agregado, file = "Matriz_Ciudad_Agregado_2.Rda")

# 3. COMPUTE STRAIGHT-LINE DISTANCES BETWEEN CENTROIDS (in kilometers)
Ciudad_agregado$Distancia_km <- distHaversine(
  cbind(Ciudad_agregado$origen_X, Ciudad_agregado$origen_Y),
  cbind(Ciudad_agregado$destino_X, Ciudad_agregado$destino_Y)
) / 1000

# 4. LOAD NEAREST PUBLIC TRANSPORT STOP FOR EACH DISTRICT
# NOTE: This mapping can be improved using GIS or direct coordinate comparison in R
centroide_parada <- read_csv("centroide_parada_3.csv")
centroide_parada <- centroide_parada[, colSums(is.na(centroide_parada)) < nrow(centroide_parada)]

# 5. LOAD AND MERGE GTFS FILES FOR THE CITY (metro + bus)
metro_gtfs <- gtfstools::read_gtfs("bilbaometro_gtfs.zip")
bus <- gtfstools::read_gtfs("bilbobus_gtfs.zip")

gtfs_list <- list(metro_gtfs, bus)
merged_gtfs <- gtfstools::merge_gtfs(metro_gtfs, bus)
stops_df <- as.data.frame(merged_gtfs$stops)

# Save merged GTFS feed
filename <- file.path("Ciudad_todos_gtfs.zip")
write_gtfs(merged_gtfs, filename)

# Prepare GTFS for routing
f <- file.path("Ciudad_todos_gtfs.zip")
gtfs <- extract_gtfs(f, quiet = TRUE)
gtfs <- gtfs_transfer_table(gtfs, d_limit = 1000)  # optional: limit distance between valid transfers
stops_df <- as.data.frame(gtfs$stops)

# 6. PREPARE DATAFRAMES TO STORE RESULTS
origenes_ciudad <- unique(Ciudad_agregado$origen)

# Trip-level result (if using gtfs_traveltimes directly)
total_tiempos <- data.frame(
  start_time = character(), duration = character(), ntransfers = integer(),
  stop_id = character(), stop_name = character(), stop_lon = numeric(), stop_lat = numeric(),
  stringsAsFactors = FALSE
)

# Aggregated travel times for OD pairs
total_tiempos_2 <- data.frame(
  origin = character(), destination = character(),
  duration = numeric(), lines_taken = character()
)

# 7. LOAD GTFS TIMETABLE FOR MONDAY
gtfs <- gtfs_timetable(gtfs, day = "Monday")

# Sort OD pairs by total weighted trips (descending)
Ciudad_ordenado <- Ciudad_agregado[order(-Ciudad_agregado$viajes_km), ]

# 8. DEFINE FUNCTION TO COMPUTE TRAVEL TIME AND LINES USED
calculate_time <- function(orig, destin, viaj) {
  origen_estacion <- centroide_parada[centroide_parada$InputID == orig, ]$TargetID
  destino_estacion <- centroide_parada[centroide_parada$InputID == destin, ]$TargetID
  
  b <- gtfs_route(
    gtfs,
    from = as.character(origen_estacion),
    to = as.character(destino_estacion),
    from_to_are_ids = TRUE,
    grep_fixed = FALSE,
    day = 'monday',
    start_time = 8 * 3600 + 120  # Start at 08:02 AM
  )
  
  lineas <- paste(unique(b$route_name), collapse = ",")
  tiempo_ruta <- b$arrival_time[length(b$arrival_time)]
  d <- chron(times = tiempo_ruta)
  total_minutes <- hours(d) * 60 + minutes(d) + seconds(d) / 60
  
  resultados <- list("tiempo" = total_minutes, "lineas" = lineas, "Origen" = orig, "Destino" = destin, "Viajes" = viaj)
  return(resultados)
}

# ========================================================================
# PARALLEL COMPUTATION TO ESTIMATE TRAVEL TIMES FOR MOST RELEVANT FLOWS
# ========================================================================

# Use multiple CPU cores to speed up travel time computation
cores <- parallel::detectCores()
cl <- makeCluster(cores - 1)
registerDoParallel(cl)

# Export necessary variables and functions to each worker node
clusterExport(cl, c("Ciudad_ordenado", "calculate_time", "centroide_parada", "gtfs"))

# Optional: convert origin-destination pairs to list (reserved for future use)
Ciudad_ordenado_list <- as.list(Ciudad_ordenado[c("origen", "destino")])

# Run travel time calculation in parallel for **todos** los pares OD
top_n <- nrow(Ciudad_ordenado)
start_time <- Sys.time()
tiempos_df <- foreach(k = seq_len(top_n),
                      .packages = c("gtfsrouter", "chron"),
                      .combine  = dplyr::bind_rows) %dopar% {
                        res <- tryCatch(
                          calculate_time(Ciudad_ordenado$origen[k],
                                         Ciudad_ordenado$destino[k],
                                         Ciudad_ordenado$viajes[k]),
                          error = function(e) NULL)
                        if (is.null(res)) return(NULL)
                        tibble::as_tibble(res)
                      }
end_time <- Sys.time()
stopCluster(cl)
print(end_time - start_time)

# Renombramos para el join
names(tiempos_df) <- c("tiempo_TP", "lineas", "origen", "destino", "viajes")

# ======================================================================================
# ASSIGN ESTIMATED PUBLIC TRANSPORT TIMES TO THE ORIGINAL ORIGIN-DESTINATION MATRIX
# ======================================================================================

for (j in 1:nrow(Ciudad_agregado)) {
  origen_estacion <- centroide_parada[centroide_parada$InputID == Ciudad_agregado$origen[j], ]$TargetID
  destino_estacion <- centroide_parada[centroide_parada$InputID == Ciudad_agregado$destino[j], ]$TargetID
  origen_estacion_name <- subset(stops_df, stop_id == origen_estacion)$stop_name[1]
  destino_estacion_name <- subset(stops_df, stop_id == destino_estacion)$stop_name[1]
  
  tiempo <- total_tiempos[total_tiempos$origen == origen_estacion & total_tiempos$stop_id == destino_estacion, ]$duration[1]
  
  d <- chron(times = tiempo)
  total_minutes <- hours(d) * 60 + minutes(d) + seconds(d) / 60
  
  Ciudad_agregado$minutes[j] <- total_minutes
  Ciudad_agregado$mult2[j] <- Ciudad_agregado$duration[j] * Ciudad_agregado$viajes[j]
}

# Use the travel time tibble produced in the parallel step
tabla <- tiempos_df %>%
  dplyr::mutate(
    tiempo_TP = as.numeric(tiempo_TP),
    origen    = as.character(origen),
    destino   = as.character(destino)
  )

# Reload the aggregated OD matrix
load("Matriz_Ciudad_Agregado_2.Rda")

# (Optional) recalculate straight line distances
for (j in seq_len(nrow(Ciudad_agregado))) {
  Ciudad_agregado$Distancia_km[j] <- distm(
    c(Ciudad_agregado$origen_Y[j],  Ciudad_agregado$origen_X[j]),
    c(Ciudad_agregado$destino_Y[j], Ciudad_agregado$destino_X[j]),
    fun = distHaversine
  ) / 1000
}

# Cast keys to character before the join
Ciudad_agregado$origen  <- as.character(Ciudad_agregado$origen)
Ciudad_agregado$destino <- as.character(Ciudad_agregado$destino)

# Merge public transport travel time into the OD matrix
Ciudad_agregado <- dplyr::left_join(
  Ciudad_agregado,
  tabla %>% dplyr::select(origen, destino, tiempo_TP),
  by = c("origen", "destino")
)

# Filter only rows with valid public transport time
library(tidyverse)
before <- sum(Ciudad_agregado$viajes_km)
Ciudad_agregado <- Ciudad_agregado %>% drop_na(tiempo_TP)
after <- sum(Ciudad_agregado$viajes_km)
(after / before) * 100  # % of valid data retained

# ============================================================
# CALCULATE PRIVATE VEHICLE COST AND TIME FOR EACH OD PAIR
# ============================================================

n_days_month <- 20
cost_VP_km <- 0.15      # €/km
months <- 12
speed <- 35             # km/h assumed

Ciudad_agregado$coste_VP <- Ciudad_agregado$Distancia_km * n_days_month * cost_VP_km * months * 2
Ciudad_agregado$tiempo_VP <- (Ciudad_agregado$Distancia_km / speed) * 60

# ======================================================
# ASSIGN AVERAGE INCOME PER DISTRICT BASED ON POSTCODE
# ======================================================

MITMA_CP <- read_csv("MITMA_CP_correspondencia.csv", col_types = cols(
  InputID = col_character(),
  TargetID = col_character(),
  Distance = col_double()
))

renta_CP <- read.csv("cp_conrenta.csv", encoding = "UTF-8", sep = ";", colClasses = c("COD_POSTAL" = "character"))

Ciudad_agregado$Renta <- NA  

for (j in 1:nrow(Ciudad_agregado)) {
  
  CP <- as.character(MITMA_CP[MITMA_CP$InputID == Ciudad_agregado$origen[j], ]$TargetID[1])
  
  if (!is.na(CP)) {
    renta_match <- renta_CP[renta_CP$COD_POSTAL == CP, ]$`Renta media por hogar`
    
    if (length(renta_match) > 0) {
      Ciudad_agregado$Renta[j] <- renta_match[1]
    } else {
      message(paste("No income data found for origin", Ciudad_agregado$origen[j], "with CP =", CP))
      Ciudad_agregado$Renta[j] <- NA
    }
  } else {
    message(paste("No postal code available for origin", Ciudad_agregado$origen[j]))
    Ciudad_agregado$Renta[j] <- NA
  }
}

Ciudad_agregado$Renta <- as.double(Ciudad_agregado$Renta)
Ciudad_agregado$Porcentaje_Renta <- Ciudad_agregado$coste_VP / (Ciudad_agregado$Renta / 2.2)

# ======================================================
# DEFINE TRANSPORT POVERTY CATEGORIES BASED ON THRESHOLDS
# ======================================================

Ciudad_agregado$Pobreza <- "0"

for (i in 1:nrow(Ciudad_agregado)) {
  if (!is.na(Ciudad_agregado$tiempo_TP[i]) && Ciudad_agregado$tiempo_TP[i] > 35) {
    if (!is.na(Ciudad_agregado$tiempo_VP[i]) && Ciudad_agregado$tiempo_VP[i] < 35) {
      if (!is.na(Ciudad_agregado$Porcentaje_Renta[i]) && Ciudad_agregado$Porcentaje_Renta[i] > 0.1) {
        Ciudad_agregado$Pobreza[i] <- "A"
      } else {
        Ciudad_agregado$Pobreza[i] <- "C"
      }
    } else {
      if (!is.na(Ciudad_agregado$Porcentaje_Renta[i]) && Ciudad_agregado$Porcentaje_Renta[i] > 0.1) {
        Ciudad_agregado$Pobreza[i] <- "B"
      } else {
        Ciudad_agregado$Pobreza[i] <- "D"
      }
    }
  }
}

Ciudad_agregado$tiempo_extra <- Ciudad_agregado$tiempo_TP - 35

# ================================================================
# AGGREGATE POVERTY CATEGORIES AND EXCESS TIME BY ORIGIN DISTRICT
# ================================================================

groupColumns <- c("origen", "Pobreza")
dataColumns <- c("viajes", "tiempo_extra")
Resumen_origenes <- ddply(Ciudad_agregado, groupColumns, function(x) colSums(x[dataColumns]))

Resumen_origenes_2 <- data.frame(
  origen = character(),
  Pobreza_A = numeric(),
  Pobreza_B = numeric(),
  Pobreza_C = numeric(),
  Pobreza_D = numeric(),
  Tiempo_extra = numeric(),
  stringsAsFactors = FALSE
)

lista_origenes <- unique(Resumen_origenes$origen)

for (j in 1:length(lista_origenes)) {
  Pobreza_A <- sum(Resumen_origenes[Resumen_origenes$Pobreza == "A" & Resumen_origenes$origen == lista_origenes[j], ]$viajes) /
    sum(Resumen_origenes[Resumen_origenes$origen == lista_origenes[j], ]$viajes)
  Pobreza_B <- sum(Resumen_origenes[Resumen_origenes$Pobreza == "B" & Resumen_origenes$origen == lista_origenes[j], ]$viajes) /
    sum(Resumen_origenes[Resumen_origenes$origen == lista_origenes[j], ]$viajes)
  Pobreza_C <- sum(Resumen_origenes[Resumen_origenes$Pobreza == "C" & Resumen_origenes$origen == lista_origenes[j], ]$viajes) /
    sum(Resumen_origenes[Resumen_origenes$origen == lista_origenes[j], ]$viajes)
  Pobreza_D <- sum(Resumen_origenes[Resumen_origenes$Pobreza == "D" & Resumen_origenes$origen == lista_origenes[j], ]$viajes) /
    sum(Resumen_origenes[Resumen_origenes$origen == lista_origenes[j], ]$viajes)
  
  Tiempo_extra <- (sum(Resumen_origenes[Resumen_origenes$origen == lista_origenes[j], ]$tiempo_extra) /
                     sum(Resumen_origenes[Resumen_origenes$origen == lista_origenes[j], ]$viajes)) * 60
  
  result <- data.frame(lista_origenes[j], Pobreza_A, Pobreza_B, Pobreza_C, Pobreza_D, Tiempo_extra)
  Resumen_origenes_2 <- rbind(Resumen_origenes_2, result)
}

# ======================================================================================
# FINAL ANALYTICAL SUMMARY - TRANSPORT POVERTY STATISTICS
# ======================================================================================

cat("\n========== RESUMEN FINAL: Pobreza de Transporte en Bilbao ==========\n\n")

# Total number of origin-destination pairs analyzed
total_OD <- nrow(Ciudad_agregado)
cat("Total de pares origen-destino procesados: ", total_OD, "\n")

# Number of districts evaluated
n_distritos <- length(unique(Ciudad_agregado$origen))
cat("Número de distritos evaluados: ", n_distritos, "\n\n")

# Percentage of OD pairs with public transport time > 35 min
exceso_tiempo <- sum(Ciudad_agregado$tiempo_TP > 35, na.rm = TRUE)
porcentaje_exceso <- round(100 * exceso_tiempo / total_OD, 2)
cat("Porcentaje de relaciones con tiempo en transporte público superior a 35 min: ", porcentaje_exceso, "%\n")

# Distribution of poverty categories (A, B, C, D)
cat("\nDistribución de categorías de pobreza:\n")
print(round(prop.table(table(Ciudad_agregado$Pobreza)) * 100, 2))

# Average travel time by transport mode
tiempo_medio_TP <- round(mean(Ciudad_agregado$tiempo_TP, na.rm = TRUE), 2)
tiempo_medio_VP <- round(mean(Ciudad_agregado$tiempo_VP, na.rm = TRUE), 2)
cat("\nTiempo medio de viaje:\n")
cat("  - Transporte público: ", tiempo_medio_TP, "min\n")
cat("  - Vehículo privado:   ", tiempo_medio_VP, "min\n")

# Estimated average annual cost using private vehicle (round trip)
coste_medio <- round(mean(Ciudad_agregado$coste_VP, na.rm = TRUE), 2)
cat("\nCoste medio anual estimado en vehículo privado (ida y vuelta): ", coste_medio, "€\n")

# % of households exceeding the 10% income threshold for transport spending
porcentaje_supera_10 <- round(100 * sum(Ciudad_agregado$Porcentaje_Renta > 0.1, na.rm = TRUE) / total_OD, 2)
cat("Porcentaje de casos donde el gasto en coche supera el 10% de la renta estimada del hogar: ", porcentaje_supera_10, "%\n")

# Districts with highest proportion of category A poverty
cat("\nTOP distritos con mayor proporción de pobreza tipo A:\n")
Resumen_origenes_2$origen <- as.character(Resumen_origenes_2$lista_origenes.j.)
top_A <- Resumen_origenes_2[order(-Resumen_origenes_2$Pobreza_A), c("origen", "Pobreza_A")]
print(head(top_A, 5))

cat("\n====================================================================\n")

