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
import ru.quipy.logic.UserAggregateState
import ru.quipy.streams.AggregateSubscriptionsManager
import java.util.*
import javax.annotation.PostConstruct

@Component
class TaskInfoProjection (
    private val taskInfoRepository: TaskInfoRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val projectEsService: EventSourcingService<UUID, ProjectAggregate, ProjectAggregateState>,
    private val userEsService: EventSourcingService<UUID, UserAggregate, UserAggregateState>
    ){
        private val logger = LoggerFactory.getLogger(TaskInfoProjection::class.java)

        @PostConstruct
        fun init() {
            subscriptionsManager.createSubscriber(ProjectAggregate::class, "taskInformation::task-information-cache") {
                `when`(TaskCreatedEvent::class) { event ->
                    var taskInfo = taskInfoRepository.findByIdOrNull(event.taskId)
                    if (taskInfo == null)
                        taskInfo = TaskInfo(event.taskId, event.taskName)
                    taskInfoRepository.save(taskInfo)

                    logger.info("Update task information projection, create task ${event.taskId}")
                }
                `when`(TagAssignedToTaskEvent::class) { event ->
                    var taskInfo = taskInfoRepository.findByIdOrNull(event.taskId)

                    val project = projectEsService.getState(event.projectId) ?: throw Exception()
                    val task = project.tasks[event.taskId] ?: throw Exception()

                    val tag = project.projectTags[event.tagId] ?: throw Exception()
                    if (taskInfo == null)
                        taskInfo = TaskInfo(event.taskId, task.name)
                    taskInfo!!.tag[event.tagId] = TagInfo(event.tagId, tag.name, tag.color)

                    taskInfoRepository.save(taskInfo!!)
                    logger.info("Update task information projection, assign tag to task ${event.tagId}-${event.taskId}")
                }
                `when`(TaskRenamedEvent::class) { event ->
                    var taskInfo = taskInfoRepository.findByIdOrNull(event.taskId)
                    if (taskInfo == null)
                        taskInfo = TaskInfo(event.taskId, event.taskName)
                    taskInfo!!.name = event.taskName
                    taskInfoRepository.save(taskInfo!!)

                    logger.info("Update task information projection, task name change ${event.taskId}-${event.taskName}")
                }
                `when`(UserAssignedToTaskEvent::class) { event ->
                    var taskInfo = taskInfoRepository.findByIdOrNull(event.taskId)

                    val project = projectEsService.getState(event.projectId) ?: throw Exception()
                    val task = project.tasks[event.taskId] ?: throw Exception()

                    if (taskInfo == null)
                        taskInfo = TaskInfo(event.taskId, task.name)

                    val user = userEsService.getState(event.userId)
                    if (user != null) {
                        taskInfo!!.performers[event.userId] = TaskPerformer(event.userId, user.name)
                    }
                    withContext(Dispatchers.IO) {
                        taskInfoRepository.save(taskInfo!!)
                    }
                    logger.info("Update task information projection, assign user to task ${event.userId}-${event.taskId}")
                }
            }
        }
    fun getById(taskId: UUID) : TaskInfo? {
        return taskInfoRepository.findByIdOrNull(taskId)
    }
}

@Document("task-information-projection")
data class TaskInfo(
        @Id
        var taskId: UUID,
        var name: String,
        var tag: MutableMap<UUID, TagInfo> = mutableMapOf(),
        val performers: MutableMap<UUID, TaskPerformer> = mutableMapOf()
)

data class TagInfo(
        val tagId: UUID,
        var name: String,
        var color: String
)

data class TaskPerformer(
        val userId: UUID,
        var name: String
)

@Repository
interface TaskInfoRepository: MongoRepository<TaskInfo, UUID>