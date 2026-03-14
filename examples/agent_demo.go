package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/coder/websocket"
)

type jsonMap map[string]any

func env(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func mustAPIKey() string {
	value := os.Getenv("PM_AGENT_API_KEY")
	if value == "" {
		fmt.Fprintln(os.Stderr, "PM_AGENT_API_KEY is required.")
		os.Exit(1)
	}
	return value
}

func requestJSON(client *http.Client, method, requestURL string, apiKey string, body any) (jsonMap, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, requestURL, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var payload jsonMap
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s failed with %d: %v", requestURL, resp.StatusCode, payload["error"])
	}
	return payload, nil
}

func logEvent(label string, payload any) {
	encoded, _ := json.MarshalIndent(payload, "", "  ")
	fmt.Printf("\n[%s]\n%s\n", label, string(encoded))
}

func main() {
	baseURL := env("PM_BASE_URL", "http://127.0.0.1:3002/prediction-markets")
	apiKey := mustAPIKey()
	placeOrder := os.Getenv("PM_PLACE_ORDER") == "1"
	orderSide := env("PM_ORDER_SIDE", "buy_yes")
	orderPriceTenths := env("PM_ORDER_PRICE_TENTHS", "543")
	orderSizeShares := env("PM_ORDER_SIZE_SHARES", "1")

	httpClient := &http.Client{Timeout: 10 * time.Second}

	for _, path := range []string{"/api/status", "/api/markets/current", "/api/orders", "/api/fills", "/api/positions", "/api/claims"} {
		payload, err := requestJSON(httpClient, http.MethodGet, baseURL+path, apiKey, nil)
		if err != nil {
			panic(err)
		}
		logEvent(path, payload)
	}

	wsURL := baseURL
	if len(wsURL) >= 5 && wsURL[:5] == "https" {
		wsURL = "wss" + wsURL[5:]
	} else {
		wsURL = "ws" + wsURL[4:]
	}
	u, err := url.Parse(wsURL + "/ws/agent")
	if err != nil {
		panic(err)
	}
	query := u.Query()
	query.Set("api_key", apiKey)
	u.RawQuery = query.Encode()

	conn, _, err := websocket.Dial(nil, u.String(), nil)
	if err != nil {
		panic(err)
	}
	defer conn.CloseNow()
	fmt.Println("\n[agent ws] connected")

	for {
		_, data, err := conn.Read(nil)
		if err != nil {
			panic(err)
		}
		var message jsonMap
		if err := json.Unmarshal(data, &message); err != nil {
			continue
		}
		msgType, _ := message["type"].(string)
		payload, _ := message["payload"].(map[string]any)
		logEvent(msgType, payload)

		if msgType == "agent.bootstrap" {
			marketID, _ := payload["market_id"].(string)
			for _, subscribeType := range []string{"book.subscribe", "positions.subscribe", "nonce_invalidations.subscribe"} {
				frame, _ := json.Marshal(jsonMap{
					"type": subscribeType,
					"payload": jsonMap{
						"market_id": marketID,
					},
				})
				if err := conn.Write(nil, websocket.MessageText, frame); err != nil {
					panic(err)
				}
			}

			if placeOrder {
				currentRound, _ := payload["current_round"].(map[string]any)
				endMs, _ := currentRound["endMs"].(float64)
				frame, _ := json.Marshal(jsonMap{
					"type": "order.place",
					"payload": jsonMap{
						"market_id":       marketID,
						"side":            orderSide,
						"price_tenths":    orderPriceTenths,
						"size_shares":     orderSizeShares,
						"expiry_ms":       int64(endMs) + 30000,
						"client_order_id": fmt.Sprintf("go-demo-%d", time.Now().UnixMilli()),
					},
				})
				if err := conn.Write(nil, websocket.MessageText, frame); err != nil {
					panic(err)
				}
			}
		}

		if msgType == "stream.recovery_required" {
			fmt.Println("Recovery required. Rebuild local state from the next snapshot.")
		}
	}
}
