package queue

import (
	"container/list"

	"github.com/gogo/protobuf/types"
	"github.com/stackrox/rox/pkg/logging"
	"github.com/stackrox/rox/pkg/sync"
)

var (
	log = logging.LoggerForModule()
)

type DeploymentObservation struct {
	DeploymentID   string
	InObservation  bool
	ObservationEnd *types.Timestamp
}

type DeploymentObservationQueue struct {
	mutex         sync.Mutex
	queue         *list.List
	deploymentMap map[string]*list.Element
}

func NewObservationQueue() *DeploymentObservationQueue {
	return &DeploymentObservationQueue{
		queue:         list.New(),
		deploymentMap: make(map[string]*list.Element),
	}
}

func (q *DeploymentObservationQueue) InObservation(deploymentID string) bool {
	deployMap, found := q.deploymentMap[deploymentID]

	// if we didn't find the deployment or the map points to nil, then we are
	// not in observation
	return !(found && deployMap == nil)
}

func (q *DeploymentObservationQueue) Pull() *DeploymentObservation {
	log.Info("SHREWS -> pull")
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.queue.Len() == 0 {
		return nil
	}

	dep := q.queue.Remove(q.queue.Front()).(*DeploymentObservation)

	// Keep the deployment in the map, so we know that we have processed this deployment.
	q.deploymentMap[dep.DeploymentID] = nil

	return dep
}

func (q *DeploymentObservationQueue) Peek() *DeploymentObservation {
	log.Info("SHREWS -> peek")
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.queue.Len() == 0 {
		return nil
	}

	return q.queue.Front().Value.(*DeploymentObservation)
}

// Push attempts to add an item to the queue, and does nothing if object already exists.
func (q *DeploymentObservationQueue) Push(observation *DeploymentObservation) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// already observing or observed this deployment
	if _, found := q.deploymentMap[observation.DeploymentID]; found {
		return
	}
	log.Infof("SHREWS -> push -- %s", observation)
	depObj := q.queue.PushBack(observation)
	q.deploymentMap[observation.DeploymentID] = depObj

}

func (q *DeploymentObservationQueue) RemoveDeployment(deploymentID string) {
	log.Infof("SHREWS -> removeDeployment %s", deploymentID)
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// The deployment is kept in the map after it has been processed to ensure we
	// do not process it again.  In that case the depObj will be nil
	depObj, found := q.deploymentMap[deploymentID]
	if !found {
		return
	}

	// Remove the object from the queue if it is not nil.
	if depObj != nil {
		q.queue.Remove(depObj)
	}
	delete(q.deploymentMap, deploymentID)
}
