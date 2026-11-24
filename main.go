package main

import (
	_ "time/tzdata"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// Incident struct matches the JSON data from the NCDOT feed.
type Incident struct {
	ID                int     `json:"ID"`
	Latitude          float64 `json:"Latitude"`
	Longitude         float64 `json:"Longitude"`
	RoadwayName       string  `json:"RoadwayName"`
	Description       string  `json:"Description"`
	EventType         string  `json:"EventType"`
	EventSubType      string  `json:"EventSubType"`
	Severity          string  `json:"Severity"`
	DirectionOfTravel string  `json:"DirectionOfTravel"`
	County            string  `json:"County"`
	StartDate         int64   `json:"StartDate"`
	LastUpdated       int64   `json:"LastUpdated"`
	StartTime         string  `json:"start"`
	Location          string  `json:"location"`
	Reason            string  `json:"reason"`
}

// --- Structs for the National Weather Service (NWS) API ---
type NWSPointsResponse struct {
	Properties struct {
		ForecastHourly string `json:"forecastHourly"`
	} `json:"properties"`
}

type NWSHourlyResponse struct {
	Properties struct {
		Periods []WeatherData `json:"periods"`
	} `json:"properties"`
}

type WeatherData struct {
	Temperature   int    `json:"temperature"`
	WindSpeed     string `json:"windSpeed"`
	ShortForecast string `json:"shortForecast"`
	Icon          string `json:"icon"`
}

func sanitizeURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "invalid-url"
	}
	return u.Host + u.Path
}

// getWeatherForIncident fetches current weather conditions from the NWS API.
func getWeatherForIncident(lat, lon float64) (*WeatherData, error) {
	pointsURL := fmt.Sprintf("https://api.weather.gov/points/%.4f,%.4f", lat, lon)
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", pointsURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "(patrolx, mtickle@gmail.com)")

	pointsResp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch NWS points data: %w", err)
	}
	defer pointsResp.Body.Close()
	if pointsResp.StatusCode != 200 {
		return nil, fmt.Errorf("NWS points API returned non-200 status: %s", pointsResp.Status)
	}
	body, err := io.ReadAll(pointsResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read NWS points response body: %w", err)
	}
	var pointsResponse NWSPointsResponse
	if err := json.Unmarshal(body, &pointsResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal NWS points JSON: %w", err)
	}
	if pointsResponse.Properties.ForecastHourly == "" {
		return nil, fmt.Errorf("NWS points response did not contain a forecast URL")
	}

	req, err = http.NewRequest("GET", pointsResponse.Properties.ForecastHourly+"?units=us", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "(patrolx, mtickle@gmail.com)")
	hourlyResp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch NWS hourly data: %w", err)
	}
	defer hourlyResp.Body.Close()
	if hourlyResp.StatusCode != 200 {
		return nil, fmt.Errorf("NWS hourly API returned non-200 status: %s", hourlyResp.Status)
	}
	hourlyBody, err := io.ReadAll(hourlyResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read NWS hourly response body: %w", err)
	}
	var hourlyResponse NWSHourlyResponse
	if err := json.Unmarshal(hourlyBody, &hourlyResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal NWS hourly JSON: %w", err)
	}
	if len(hourlyResponse.Properties.Periods) > 0 {
		return &hourlyResponse.Properties.Periods[0], nil
	}
	return nil, fmt.Errorf("no weather periods returned from NWS")
}

// saveToUnifiedDB normalizes, enriches, and saves an incident to the unified table, returning true if newly inserted.
func saveToUnifiedDB(db *sql.DB, incident Incident) (bool, error) {
	source := "NCDOT"
	sourceID := strconv.Itoa(incident.ID)
	eventType := incident.EventType

	var parsedTime time.Time

	// Use the int64 LastUpdated field provided by the v2 API
	if incident.LastUpdated > 0 {
		// DriveNC v2 timestamps are typically 13-digit Unix milliseconds
		if incident.LastUpdated > 9999999999 {
			parsedTime = time.UnixMilli(incident.LastUpdated)
		} else {
			// Fallback in case they switch to 10-digit seconds
			parsedTime = time.Unix(incident.LastUpdated, 0)
		}
	} else if incident.StartDate > 0 {
		if incident.StartDate > 9999999999 {
			parsedTime = time.UnixMilli(incident.StartDate)
		} else {
			parsedTime = time.Unix(incident.StartDate, 0)
		}
	} else {
		slog.Warn("timestamp_missing_fallback",
			"incident_id", incident.ID,
			"county", incident.County,
			"roadway", incident.RoadwayName,
		)
		parsedTime = time.Now()
	}

	// --- ENRICHMENT STEP ---
	weatherStart := time.Now()
	weatherData, err := getWeatherForIncident(incident.Latitude, incident.Longitude)
	if err != nil {
		slog.Warn("nws_enrichment_failed",
			"incident_id", incident.ID,
			"county", incident.County,
			"lat", incident.Latitude,
			"lng", incident.Longitude,
			"duration_ms", time.Since(weatherStart).Milliseconds(),
			"error", err,
		)
	} else {
		slog.Info("nws_enrichment_ok",
			"incident_id", incident.ID,
			"county", incident.County,
			"temp_f", weatherData.Temperature,
			"forecast", weatherData.ShortForecast,
			"duration_ms", time.Since(weatherStart).Milliseconds(),
		)
	}

	details := map[string]interface{}{
		"raw_incident": incident,
		"weather":      weatherData,
	}

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return false, fmt.Errorf("could not marshal unified details to JSON: %w", err)
	}

	// --- PREPARE NEW COLUMN VALUES ---
	var weatherTemp sql.NullInt32
	var weatherWind, weatherForecast sql.NullString

	if weatherData != nil {
		weatherTemp.Int32 = int32(weatherData.Temperature)
		weatherTemp.Valid = true
		weatherWind.String = weatherData.WindSpeed
		weatherWind.Valid = true
		weatherForecast.String = weatherData.ShortForecast
		weatherForecast.Valid = true
	}

	// RETURNING (xmax = 0) distinguishes brand-new inserts (true) from conflict updates (false)
	sqlStatement := `
		INSERT INTO unified_incidents (
			source, source_id, event_type, status, address, latitude, longitude, timestamp, details,
			problem_detail, weather_temp, weather_wind_speed, weather_forecast
		) VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (source, source_id) DO UPDATE SET
			details = EXCLUDED.details,
			status = 'active',
			problem_detail = EXCLUDED.problem_detail,
			weather_temp = EXCLUDED.weather_temp,
			weather_wind_speed = EXCLUDED.weather_wind_speed,
			weather_forecast = EXCLUDED.weather_forecast
		RETURNING (xmax = 0) AS inserted;
	`

	var wasInserted bool
	err = db.QueryRow(sqlStatement,
		source, sourceID, eventType, incident.Location, incident.Latitude, incident.Longitude, parsedTime, detailsJSON,
		incident.Reason, weatherTemp, weatherWind, weatherForecast,
	).Scan(&wasInserted)

	return wasInserted, err
}

func main() {
	cycleStart := time.Now()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if err := godotenv.Load(); err != nil {
		slog.Debug("dotenv_not_loaded", "reason", "using container/environment variables")
	}

	dbHost := os.Getenv("DATABASE_HOST")
	dotURL := os.Getenv("DOT_URL")
	if dotURL == "" {
		slog.Error("missing_required_env", "var", "DOT_URL")
		os.Exit(1)
	}

	slog.Info("ingest_cycle_started",
		"service", "ncdot-ingestor-bot",
		"db_host", dbHost,
		"feed_endpoint", sanitizeURL(dotURL),
	)

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		dbHost, os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_USERNAME"),
		os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		slog.Error("db_open_failed", "db_host", dbHost, "error", err)
		os.Exit(1)
	}
	defer db.Close()

	dbStart := time.Now()
	if err := db.Ping(); err != nil {
		slog.Error("db_ping_failed", "db_host", dbHost, "error", err)
		os.Exit(1)
	}
	slog.Info("db_connected", "db_host", dbHost, "latency_ms", time.Since(dbStart).Milliseconds())

	fetchStart := time.Now()
	httpClient := &http.Client{Timeout: 20 * time.Second}
	resp, err := httpClient.Get(dotURL)
	if err != nil {
		slog.Error("ncdot_fetch_failed", "endpoint", sanitizeURL(dotURL), "error", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Error("ncdot_non_200_status", "status_code", resp.StatusCode, "status", resp.Status)
		os.Exit(1)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("ncdot_body_read_failed", "error", err)
		os.Exit(1)
	}

	var allIncidents []Incident
	if err := json.Unmarshal(body, &allIncidents); err != nil {
		slog.Error("ncdot_json_unmarshal_failed",
			"payload_bytes", len(body),
			"error", err,
		)
		os.Exit(1)
	}

	slog.Info("ncdot_feed_fetched",
		"status_code", resp.StatusCode,
		"payload_bytes", len(body),
		"total_raw_incidents", len(allIncidents),
		"fetch_duration_ms", time.Since(fetchStart).Milliseconds(),
	)

	insertedCount := 0
	updatedCount := 0
	skippedCount := 0
	failedCount := 0

	for _, incident := range allIncidents {
		subType := strings.ToLower(incident.EventSubType)

		// 1. All "accidentsAndIncidents"
		// 2. All "closures" where the subtype includes "vehiclecrash"
		if incident.EventType == "accidentsAndIncidents" || (incident.EventType == "closures" && strings.Contains(subType, "vehiclecrash")) {
			wasInserted, err := saveToUnifiedDB(db, incident)
			if err != nil {
				failedCount++
				slog.Error("ncdot_incident_upsert_failed",
					"incident_id", incident.ID,
					"county", incident.County,
					"roadway", incident.RoadwayName,
					"event_type", incident.EventType,
					"error", err,
				)
				continue
			}

			action := "updated"
			if wasInserted {
				insertedCount++
				action = "inserted"
			} else {
				updatedCount++
			}

			slog.Info("ncdot_incident_synced",
				"action", action,
				"incident_id", incident.ID,
				"county", incident.County,
				"roadway", incident.RoadwayName,
				"event_type", incident.EventType,
				"sub_type", incident.EventSubType,
				"severity", incident.Severity,
			)
		} else {
			skippedCount++
		}
	}

	slog.Info("ingest_cycle_completed",
		"service", "ncdot-ingestor-bot",
		"raw_total", len(allIncidents),
		"inserted", insertedCount,
		"updated", updatedCount,
		"filtered_out", skippedCount,
		"failed", failedCount,
		"total_duration_ms", time.Since(cycleStart).Milliseconds(),
	)
}
