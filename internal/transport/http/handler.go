package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/igor/fulfill-go/internal/observability"
	"github.com/igor/fulfill-go/internal/order"
)

type orderService interface {
	Create(ctx context.Context, command order.CreateCommand) (order.Order, bool, error)
	Get(ctx context.Context, id string) (order.Order, error)
}

type Handler struct {
	orders  orderService
	db      *pgxpool.Pool
	metrics *observability.Metrics
	logger  *slog.Logger
}

func NewHandler(orders orderService, db *pgxpool.Pool, metrics *observability.Metrics, logger *slog.Logger) http.Handler {
	handler := &Handler{
		orders:  orders,
		db:      db,
		metrics: metrics,
		logger:  logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", handler.createOrder)
	mux.HandleFunc("GET /v1/orders/{id}", handler.getOrder)
	mux.HandleFunc("GET /health/live", handler.live)
	mux.HandleFunc("GET /health/ready", handler.ready)
	mux.Handle("GET /metrics", promhttp.Handler())

	return otelhttp.NewHandler(handler.accessLog(mux), "http.server")
}

type createOrderRequest struct {
	CustomerID string       `json:"customer_id"`
	Items      []order.Item `json:"items"`
}

func (h *Handler) createOrder(response http.ResponseWriter, request *http.Request) {
	var input createOrderRequest
	if err := decodeJSON(response, request, &input); err != nil {
		writeError(response, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	createdOrder, isNew, err := h.orders.Create(request.Context(), order.CreateCommand{
		CustomerID:     input.CustomerID,
		IdempotencyKey: request.Header.Get("Idempotency-Key"),
		Items:          input.Items,
	})
	if err != nil {
		h.handleOrderError(response, err)
		return
	}

	status := http.StatusOK
	if isNew {
		status = http.StatusCreated
		h.metrics.OrdersCreated.Inc()
	}
	response.Header().Set("Location", "/v1/orders/"+createdOrder.ID)
	writeJSON(response, status, createdOrder)
}

func (h *Handler) getOrder(response http.ResponseWriter, request *http.Request) {
	found, err := h.orders.Get(request.Context(), request.PathValue("id"))
	if err != nil {
		h.handleOrderError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, found)
}

func (h *Handler) live(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ready(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), time.Second)
	defer cancel()
	if err := h.db.Ping(ctx); err != nil {
		writeError(response, http.StatusServiceUnavailable, "not_ready", "database is unavailable")
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) handleOrderError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, order.ErrInvalidInput):
		writeError(response, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, order.ErrNotFound):
		writeError(response, http.StatusNotFound, "order_not_found", err.Error())
	case errors.Is(err, order.ErrInsufficientStock):
		writeError(response, http.StatusConflict, "insufficient_stock", err.Error())
	default:
		h.logger.Error("request failed", "error", err)
		writeError(response, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func decodeJSON(response http.ResponseWriter, request *http.Request, destination any) error {
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

func writeJSON(response http.ResponseWriter, status int, body any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(body)
}

func writeError(response http.ResponseWriter, status int, code, message string) {
	writeJSON(response, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (response *statusRecorder) WriteHeader(status int) {
	response.status = status
	response.ResponseWriter.WriteHeader(status)
}

func (h *Handler) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
		next.ServeHTTP(recorder, request)

		route := request.Pattern
		if route == "" {
			route = "unmatched"
		} else {
			route = strings.TrimSpace(strings.TrimPrefix(route, request.Method))
		}
		h.metrics.ObserveHTTP(request.Method, route, recorder.status, started)
		h.logger.Info("http request",
			"method", request.Method,
			"route", route,
			"status", recorder.status,
			"duration", time.Since(started),
		)
	})
}
