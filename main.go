package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// Incident struct matches the JSON data from the NCDOT feed.
type Incident struct {
	ID                    int     `json:"id"`
	Latitude              float64 `json:"latitude"`
	Longitude             float64 `json:"longitude"`
	CommonName            string  `json:"commonName"`
	Reason                string  `json:"reason"`
	Condition             string  `json:"condition"`
	IncidentType          string  `json:"incidentType"`
	Severity              int     `json:"severity"`
	Direction             string  `json:"direction"`
	Location              string  `json:"location"`
	CountyID              int     `json:"countyId"`
	CountyName            string  `json:"countyName"`
	City                  string  `json:"city"`
	StartTime             string  `json:"start"`
	EndTime               string  `json:"end"`
	LastUpdate            string  `json:"lastUpdate"`
	Road                  string  `json:"road"`
	RouteID               int     `json:"routeId"`
	LanesClosed           int     `json:"lanesClosed"`
	LanesTotal            int     `json:"lanesTotal"`
	Detour                string  `json:"detour"`
	CrossStreetPrefix     string  `json:"crossStreetPrefix"`
	CrossStreetNumber     int     `json:"crossStreetNumber"`
	CrossStreetSuffix     string  `json:"crossStreetSuffix"`
	CrossStreetCommonName string  `json:"crossStreetCommonName"`
	Event                 string  `json:"event"`
	CreatedFromConcurrent bool    `json:"createdFromConcurrent"`
	MovableConstruction   string  `json:"movableConstruction"`
	WorkZoneSpeedLimit    int     `json:"workZoneSpeedLimit"`
}

// saveToUnifiedDB normalizes and saves a crash incident to the unified table.
func saveToUnifiedDB(db *sql.DB, incident Incident) error {
	source := "NCDOT"
	sourceID := strconv.Itoa(incident.ID)
	eventType := "Vehicle Crash"

	// --- DEBUGGING STEP ---
	// Log the raw timestamp string to see its actual format.
	log.Printf("DEBUG: Raw StartTime string from NC DOT API for incident %d: '%s'", incident.ID, incident.StartTime)

	parsedTime, err := time.Parse(time.RFC3339, incident.StartTime)
	if err != nil {
		// Make the warning more explicit when we have to fall back.
		log.Printf("WARNING: Could not parse timestamp '%s' for incident %d. Using current time instead. Error: %v", incident.StartTime, incident.ID, err)
		parsedTime = time.Now()
	}

	detailsJSON, err := json.Marshal(incident)
	if err != nil {
		return fmt.Errorf("could not marshal incident details to JSON: %w", err)
	}

	sqlStatement := `
		INSERT INTO unified_incidents (
			source, source_id, event_type, status, address, latitude, longitude, timestamp, details
		) VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8)
		ON CONFLICT (source, source_id) DO NOTHING;
	`

	_, err = db.Exec(sqlStatement,
		source, sourceID, eventType, incident.Location, incident.Latitude, incident.Longitude, parsedTime, detailsJSON,
	)
	return err
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Note: .env file not found, reading credentials from environment")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		os.Getenv("DATABASE_HOST"), os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_USERNAME"),
		os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Error opening database: %s", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Error connecting to database: %s", err)
	}
	log.Println("Successfully connected to the database.")

	dotURL := os.Getenv("DOT_URL")
	if dotURL == "" {
		log.Fatalln("Error: DOT_URL must be set in your environment or .env file.")
	}

	resp, err := http.Get(dotURL)
	if err != nil {
		log.Fatalf("Error fetching data from NC DOT API: %s\n", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %s\n", err)
	}

	var allIncidents []Incident
	if err := json.Unmarshal(body, &allIncidents); err != nil {
		// If the API returns a non-JSON error page, this will fail.
		// Log the raw response for debugging.
		log.Printf("DEBUG: Raw response from server was: %s", string(body))
		log.Fatalf("Error unmarshalling JSON: %s\n", err)
	}

	log.Printf("Found %d total incidents from NC DOT.", len(allIncidents))
	crashesSaved := 0

	for _, incident := range allIncidents {
		if incident.IncidentType == "Vehicle Crash" {
			if err := saveToUnifiedDB(db, incident); err != nil {
				log.Printf("Error saving NC DOT crash ID %d: %v", incident.ID, err)
			} else {
				crashesSaved++
			}
		}
	}

	log.Printf("Run complete. Processed and saved %d vehicle crashes to the unified table.", crashesSaved)
}
