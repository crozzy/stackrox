package lifecycle

import (
	"container/list"

	"github.com/gogo/protobuf/types"
	"github.com/stackrox/rox/pkg/sync"
)

type deploymentObservation struct {
	deploymentID   string
	inObservation  bool
	observationEnd *types.Timestamp
}

type deploymentObservationQueue struct {
	mutex         sync.Mutex
	queue         *list.List
	deploymentMap map[string]*list.Element
}

func newObservationQueue() *deploymentObservationQueue {
	return &deploymentObservationQueue{
		queue:         list.New(),
		deploymentMap: make(map[string]*list.Element),
	}
}

func (q *deploymentObservationQueue) inObservation(deploymentID string) bool {
	log.Infof("SHREWS -> inObservation -- %s", deploymentID)
	deployMap, found := q.deploymentMap[deploymentID]

	// TODO:  come back and think about this.
	// if we didn't find the deployment or the map points to nil, then we are
	// not in observation
	if found && deployMap == nil {
		return false
	}

	return true
}

func (q *deploymentObservationQueue) isEmpty() bool {
	log.Info("SHREWS -> isEmpty")
	if q.queue.Len() == 0 {
		return true
	}

	return false
}

func (q *deploymentObservationQueue) pull() *deploymentObservation {
	log.Info("SHREWS -> pull")
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.queue.Len() == 0 {
		return nil
	}

	dep := q.queue.Remove(q.queue.Front()).(*deploymentObservation)

	// keep the deployment in the map so we know that we have processed this deployment.
	q.deploymentMap[dep.deploymentID] = nil

	log.Infof("SHREWS -> pull returned %s", dep)
	return dep
}

func (q *deploymentObservationQueue) peak() *deploymentObservation {
	log.Info("SHREWS -> peak")
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.queue.Len() == 0 {
		return nil
	}

	return q.queue.Front().Value.(*deploymentObservation)
}

// Push attempts to add an item to the queue, and does nothing if object already exists.
func (q *deploymentObservationQueue) push(observation *deploymentObservation) {
	log.Infof("SHREWS -> push -- %s", observation)
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// already observing or observed this deployment
	if _, found := q.deploymentMap[observation.deploymentID]; found {
		log.Infof("SHREWS -> push -- already have it %s", observation.deploymentID)
		return
	}

	depObj := q.queue.PushBack(observation)
	q.deploymentMap[observation.deploymentID] = depObj

}

func (q *deploymentObservationQueue) removeDeployment(deploymentID string) {
	log.Infof("SHREWS -> removeDeployment %s", deploymentID)
	q.mutex.Lock()
	defer q.mutex.Unlock()

	depObj, found := q.deploymentMap[deploymentID]
	if !found {
		return
	}

	if depObj != nil {
		q.queue.Remove(depObj)
	}
	delete(q.deploymentMap, deploymentID)
}
