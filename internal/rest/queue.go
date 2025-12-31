package rest

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"distributed-task-queue/services/queue"

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
	e.GET("/queues", handler.GetQueues)
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

	created, err := a.Service.CreateQueue(queue)
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

	queue, err := a.Service.GetQueue(id)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, queue)
}

// GetQueues will get all queues
// @Summary      Get all queues
// @Description  Retrieve all queues
// @Tags         queues
// @Accept       json
// @Produce      json
// @Success      200  {array}   domain.Queue
// @Failure      500  {string}  string  "Internal Server Error"
// @Router       /queues [get]
func (a *QueueHandler) GetQueues(c echo.Context) error {
	c.Logger().Info("GetQueues called")

	queues, err := a.Service.GetAllQueues()
	if err != nil {
		c.Logger().Errorf("Error getting queues: %v", err)
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	c.Logger().Infof("Returning %d queues", len(queues))
	return c.JSON(http.StatusOK, queues)
}
