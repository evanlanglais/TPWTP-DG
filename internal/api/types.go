package api

import "time"

// DestinationsResponse is the top-level response from GET /v1/destinations.
type DestinationsResponse struct {
	Destinations []Destination `json:"destinations"`
}

type Destination struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Slug  string `json:"slug"`
	Parks []Park `json:"parks"`
}

type Park struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// EntityChildrenResponse is the response from GET /v1/entity/{id}/children.
type EntityChildrenResponse struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	EntityType string        `json:"entityType"`
	Timezone   string        `json:"timezone"`
	Children   []ChildEntity `json:"children"`
}

type ChildEntity struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	EntityType string   `json:"entityType"`
	Slug       string   `json:"slug"`
	ParentID   string   `json:"parentId"`
	ExternalID string   `json:"externalId"`
	Location   *LatLong `json:"location,omitempty"`
}

type LatLong struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// EntityLiveResponse is the response from GET /v1/entity/{id}/live.
type EntityLiveResponse struct {
	ID       string           `json:"id"`
	Name     string           `json:"name"`
	LiveData []LiveDataEntity `json:"liveData"`
}

type LiveDataEntity struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	EntityType string          `json:"entityType"`
	ParkID     string          `json:"parkId"`
	ExternalID string          `json:"externalId"`
	Status     string          `json:"status"`
	Queue      *QueueData      `json:"queue,omitempty"`
	LastUpdate *time.Time      `json:"lastUpdated,omitempty"`
}

type QueueData struct {
	Standby        *StandbyQueue    `json:"STANDBY,omitempty"`
	ReturnTime     *ReturnTimeQueue `json:"RETURN_TIME,omitempty"`
	PaidReturnTime *ReturnTimeQueue `json:"PAID_RETURN_TIME,omitempty"`
	SingleRider    *StandbyQueue    `json:"SINGLE_RIDER,omitempty"`
}

type StandbyQueue struct {
	WaitTime *int `json:"waitTime"`
}

type ReturnTimeQueue struct {
	State       string  `json:"state"`
	ReturnStart *string `json:"returnStart"`
	ReturnEnd   *string `json:"returnEnd"`
}
