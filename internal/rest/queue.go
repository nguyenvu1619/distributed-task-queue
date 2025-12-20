package rest

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	queue "distributed-task-queue/services"

	"distributed-task-queue/domain"
)

// ResponseError represent the response error struct

// ArticleHandler  represent the httphandler for article
type QueueHandler struct {
	Service *queue.QueueService
}

// NewJobHandler will initialize the jobs/ resources endpoint
func NewQueueHandler(e *echo.Echo, svc queue.QueueService) {
	handler := &QueueHandler{
		Service: &svc,
	}
	e.POST("/queues", handler.CreateQueue)
	e.GET("/queues/:id", handler.GetQueue)
}

// PublishJob will publish the job based on given params
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

// GetByID will get article by given id
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
