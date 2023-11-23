package ru.quipy.projections
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
import java.lang.Exception
import java.util.*
import javax.annotation.PostConstruct

@Component
class ProjectTagsProjection (
    private val projectTagsRepository: ProjectTagsRepository,
    private val subscriptionsManager: AggregateSubscriptionsManager,
    private val projectEsService: EventSourcingService<UUID, ProjectAggregate, ProjectAggregateState>
){
    private val logger = LoggerFactory.getLogger(projectTagsRepository::class.java)

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(ProjectAggregate::class, "tags::project-tasks-tags") {
            `when`(ProjectCreatedEvent::class) { event ->
                var project = ProjectTags(event.projectId)
                projectTagsRepository.save(project)
                logger.info("Task status projection created for project ${event.projectId}")
            }
            `when`(TagCreatedEvent::class)  {event ->
                var dbProject = projectTagsRepository.findById(event.projectId).orElseThrow()
                dbProject.tags[event.tagId] = Tag(event.tagId, event.tagName, event.tagColor)
                projectTagsRepository.save(dbProject)
            }
            `when`(TagAssignedToTaskEvent::class) {event->
                var dbProject = projectTagsRepository.findById(event.projectId).orElseThrow()
                val project = projectEsService.getState(event.projectId) ?: throw Exception()
                val task = project.tasks[event.taskId] ?: throw Exception()
                var tagTask = TagTask(event.taskId, task.name)
                var tag = project.projectTags[event.tagId] ?: throw Exception()
                var newTag = Tag(event.tagId, tag.name, tag.color)
                newTag.tasks[event.taskId] = tagTask
                dbProject.tags[event.tagId] = newTag
                projectTagsRepository.save(dbProject)
            }
            `when`(TaskRenamedEvent::class) {event ->
                var dbProject = projectTagsRepository.findById(event.projectId).orElseThrow()
                dbProject.tags.forEach { (_, tag) ->
                    tag.tasks[event.taskId]?.name = event.taskName
                }
                projectTagsRepository.save(dbProject)
            }
            `when`(TagDeletedEvent::class) {event ->
                val dbProject = projectTagsRepository.findById(event.projectId).orElseThrow()
                dbProject.tags.remove(event.tagId)
                projectTagsRepository.save(dbProject)
            }
        }
    }

    fun getById(projectId: UUID) : ProjectTags? {
        return projectTagsRepository.findByIdOrNull(projectId)
    }
}

@Document("project-tags-projection")
data class ProjectTags(
    @Id
    var projectId: UUID,
    var tags: MutableMap<UUID, Tag> = mutableMapOf()
)

data class Tag(
    val tagId: UUID,
    var name: String,
    var color: String,
    var tasks: MutableMap<UUID, TagTask> = mutableMapOf()
)

data class TagTask (
    val taskId: UUID,
    var name: String
)


@Repository
interface ProjectTagsRepository: MongoRepository<ProjectTags, UUID>