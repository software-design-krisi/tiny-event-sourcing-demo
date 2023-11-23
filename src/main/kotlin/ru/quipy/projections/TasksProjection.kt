package ru.quipy.projections
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import ru.quipy.api.*
import ru.quipy.core.EventSourcingService
import ru.quipy.logic.ProjectAggregateState
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class TasksProjection (
    private val tasksRepository: TasksRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager,
){
    private val logger = LoggerFactory.getLogger(TasksProjection::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(ProjectAggregate::class, "tasks::project-tasks"){
            `when`(TaskCreatedEvent::class) { event ->
                var dbProjectTasks = tasksRepository.findById(event.taskId)
                var projectTasks = Task(event.taskId, event.projectId, event.taskName)
                if (!dbProjectTasks.isEmpty) {
                    projectTasks = dbProjectTasks.get()
                }
                tasksRepository.save(projectTasks)
                logger.info("Update project tasks projection, add task to project ${event.projectId}-${event.taskId}")
            }
            `when`(TaskRenamedEvent::class) { event ->
                var dbTask = tasksRepository.findByIdOrNull(event.taskId)
                if (dbTask == null) {
                    dbTask = Task(event.taskId, event.projectId, event.taskName)
                }
                dbTask.name = event.taskName
                tasksRepository.save(dbTask)
                logger.info("Update project tasks projection, renamed task ${event.taskId}")
            }
        }
    }

    fun getTasksByProjectId(projectId: UUID) : List<Task> {
        return tasksRepository.findByProjectId(projectId)
    }
}

@Document("project-tasks-projection")
data class Task(
    @Id
    var id: UUID,
    var projectId: UUID,
    var name: String
)


@Repository
interface TasksRepository: MongoRepository<Task, UUID> {
    fun findByProjectId(projectId: UUID): List<Task>
}