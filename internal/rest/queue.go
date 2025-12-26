package rest

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	queue "distributed-task-queue/services"

	"distributed-task-queue/domain"
)

// QueueHandler  represent the httphandler for queue
type QueueHandler struct {
	Service *queue.QueueService
}

// NewQueueHandler will initialize the queues/ resources endpoint
func NewQueueHandler(e *echo.Echo, svc queue.QueueService) {
	handler := &QueueHandler{
		Service: &svc,
	}
	e.POST("/queues", handler.CreateQueue)
	e.GET("/queues/:id", handler.GetQueue)
}

// CreateQueue will create a new queue
// @Summary      Create a new queue
// @Description  Create a new queue with specified configuration
// @Tags         queues
// @Accept       json
// @Produce      json
// @Param        queue  body      domain.Queue  true  "Queue object"
// @Success      200    {object}  domain.Queue
// @Failure      422    {string}  string  "Unprocessable Entity"
// @Failure      500    {string}  string  "Internal Server Error"
// @Router       /queues [post]
func (a *QueueHandler) CreateQueue(c echo.Context) error {

	var queue domain.Queue
	err := c.Bind(&queue)
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	ctx := c.Request().Context()

	created, err := a.Service.CreateQueue(ctx, queue)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, created)
}

// GetQueue will get queue by given id
// @Summary      Get queue by ID
// @Description  Retrieve a queue by its ID
// @Tags         queues
// @Accept       json
// @Produce      json
// @Param        id   path      int     true  "Queue ID"
// @Success      200  {object}  domain.Queue
// @Failure      404  {string}  string  "Not Found"
// @Failure      500  {string}  string  "Internal Server Error"
// @Router       /queues/{id} [get]
func (a *QueueHandler) GetQueue(c echo.Context) error {
	idP, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return c.JSON(http.StatusNotFound, domain.ErrNotFound.Error())
	}

	id := int64(idP)
	ctx := c.Request().Context()

	queue, err := a.Service.GetQueue(ctx, id)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, queue)
}
