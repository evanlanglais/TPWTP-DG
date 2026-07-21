package weather

// OneCallResponse is the subset of the OpenWeatherMap One Call 3.0 response
// (GET /data/3.0/onecall) that we project. The full verbatim body is persisted to
// raw_observations (I1), so fields omitted here are never lost — they can be added
// to the projection later without a re-fetch.
type OneCallResponse struct {
	Lat     float64         `json:"lat"`
	Lon     float64         `json:"lon"`
	Current *CurrentWeather `json:"current"`
	Hourly  []HourlyWeather `json:"hourly"`
}

// CurrentWeather is the `current` block: conditions valid at Dt (unix seconds, UTC).
// With units=metric, Temp/FeelsLike are °C and WindSpeed is m/s.
type CurrentWeather struct {
	Dt        int64         `json:"dt"`
	Temp      float64       `json:"temp"`
	FeelsLike float64       `json:"feels_like"`
	Humidity  int           `json:"humidity"`
	Clouds    int           `json:"clouds"`
	WindSpeed float64       `json:"wind_speed"`
	Weather   []WeatherCond `json:"weather"`
	Rain      *Precip       `json:"rain"` // present only when precipitating
	Snow      *Precip       `json:"snow"`
}

// HourlyWeather is one entry of the `hourly` forecast: conditions predicted for Dt.
// Pop is the probability of precipitation, 0..1.
type HourlyWeather struct {
	Dt      int64         `json:"dt"`
	Temp    float64       `json:"temp"`
	Pop     float64       `json:"pop"`
	Weather []WeatherCond `json:"weather"`
	Rain    *Precip       `json:"rain"`
	Snow    *Precip       `json:"snow"`
}

// WeatherCond is one OWM condition descriptor; ID encodes rain/snow/clear.
type WeatherCond struct {
	ID   int    `json:"id"`
	Main string `json:"main"`
}

// Precip is a precipitation volume block; OneH is the last/next hour's mm.
type Precip struct {
	OneH float64 `json:"1h"`
}

// PrecipMM sums rain and snow for the hour (mm); absent blocks count as 0.
func PrecipMM(rain, snow *Precip) float64 {
	var mm float64
	if rain != nil {
		mm += rain.OneH
	}
	if snow != nil {
		mm += snow.OneH
	}
	return mm
}

// CondCode returns the primary OWM condition id, or 0 if none.
func CondCode(w []WeatherCond) int {
	if len(w) > 0 {
		return w[0].ID
	}
	return 0
}

// CondMain returns the primary OWM condition group (e.g. "Rain"), or "" if none.
func CondMain(w []WeatherCond) string {
	if len(w) > 0 {
		return w[0].Main
	}
	return ""
}
